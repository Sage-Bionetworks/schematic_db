"""Synapse"""
from dataclasses import dataclass
from functools import partial
from typing import Any
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
import synapseclient as sc  # type: ignore
import pandas as pd  # type: ignore
from schematic_db.db_config import DBObjectConfig, DBDatatype


SYNAPSE_DATATYPES = {
    DBDatatype.TEXT: partial(sc.Column, columnType="LARGETEXT"),
    DBDatatype.DATE: partial(sc.Column, columnType="DATE"),
    DBDatatype.INT: partial(sc.Column, columnType="INTEGER"),
    DBDatatype.FLOAT: partial(sc.Column, columnType="DOUBLE"),
    DBDatatype.BOOLEAN: partial(sc.Column, columnType="BOOLEAN"),
}

PANDAS_DATATYPES = {DBDatatype.INT: "Int64", DBDatatype.BOOLEAN: "boolean"}


class SynapseTableNameError(Exception):
    """SynapseTableNameError"""

    def __init__(self, message: str, table_name: str) -> None:
        self.message = message
        self.table_name = table_name
        super().__init__(self.message)

    def __str__(self) -> str:
        return f"{self.message}:{self.table_name}"


class SynapseDeleteRowsError(Exception):
    """SynapseDeleteRowsError"""

    def __init__(self, message: str, table_id: str, columns: list[str]) -> None:
        self.message = message
        self.table_id = table_id
        self.columns = columns
        super().__init__(self.message)

    def __str__(self) -> str:
        return f"{self.message}; table_id:{self.table_id}; columns: {', '.join(self.columns)}"


@dataclass
class SynapseConfig:
    """A config for a Synapse Project."""

    username: str
    auth_token: str
    project_id: str


def create_synapse_column(name: str, datatype: DBDatatype) -> sc.Column:
    """Creates a Synapse column object

    Args:
        name (str): The name of the column
        datatype (DBDatatype): The datatype of the column

    Returns:
        sc.Column: _description_
    """
    func = SYNAPSE_DATATYPES[datatype]
    return func(name=name)


class Synapse:  # pylint: disable=too-many-public-methods
    """
    The Synapse class handles interactions with a project in Synapse.
    """

    def __init__(self, config: SynapseConfig) -> None:
        """Init

        Args:
            config (SynapseConfig): A SynapseConfig object
        """
        username = config.username
        auth_token = config.auth_token
        project_id = config.project_id

        syn = sc.Synapse()
        syn.login(username, authToken=auth_token)

        self.syn = syn
        self.project_id = project_id

    def get_table_names(self) -> list[str]:
        """Gets the names of the tables in the schema

        Returns:
            list[str]: A list of table names
        """
        tables = self._get_tables()
        return [table["name"] for table in tables]

    def _get_tables(self) -> list[sc.Table]:
        project = self.syn.get(self.project_id)
        return list(self.syn.getChildren(project, includeTypes=["table"]))

    def get_table_column_names(self, table_name: str) -> list[str]:
        """Gets the column names from a synapse table

        Args:
            table_name (str): The name of the table

        Returns:
            list[str]: A list of column names
        """
        synapse_id = self.get_synapse_id_from_table_name(table_name)
        table = self.syn.get(synapse_id)
        columns = list(self.syn.getTableColumns(table))
        return [column.name for column in columns]

    def get_synapse_id_from_table_name(self, table_name: str) -> str:
        """Gets the synapse id from the table name

        Args:
            table_name (str): The name of the table

        Raises:
            SynapseTableNameError: When no tables match the name
            SynapseTableNameError: When multiple tables match the name

        Returns:
            str: A synapse id
        """
        tables = self._get_tables()
        matching_tables = [table for table in tables if table["name"] == table_name]
        if len(matching_tables) == 0:
            raise SynapseTableNameError("No matching tables with name:", table_name)
        if len(matching_tables) > 1:
            raise SynapseTableNameError(
                "Multiple matching tables with name:", table_name
            )
        return matching_tables[0]["id"]

    def get_table_name_from_synapse_id(self, synapse_id: str) -> str:
        """Gets the table name from the synapse id

        Args:
            synapse_id (str): A synapse id

        Returns:
            str: The name of the table with the synapse id
        """
        tables = self._get_tables()
        return [table["name"] for table in tables if table["id"] == synapse_id][0]

    def query_table(
        self, synapse_id: str, include_row_data: bool = False
    ) -> pd.DataFrame:
        """Queries a whole table

        Args:
            synapse_id (str): The Synapse id of the table to delete
            table_config (DBObjectConfig): The config for the table
            include_row_data (bool): Include row_id and row_etag. Defaults to False.

        Returns:
            pd.DataFrame: The queried table
        """
        query = f"SELECT * FROM {synapse_id}"
        return self.execute_sql_query(query, include_row_data)

    def execute_sql_query(
        self, query: str, include_row_data: bool = False
    ) -> pd.DataFrame:
        """Execute a Sql query

        Args:
            query (str): A SQL statement that can be run by Synapse
            include_row_data (bool): Include row_id and row_etag. Defaults to False.

        Returns:
            pd.DataFrame: The queried table
        """
        result = self.execute_sql_statement(query, include_row_data)
        table = pd.read_csv(result.filepath)
        return table

    def execute_sql_statement(
        self, statement: str, include_row_data: bool = False
    ) -> Any:
        """Execute a SQL statement

        Args:
            statement (str): A SQL statement that can be run by Synapse
            include_row_data (bool): Include row_id and row_etag. Defaults to False.

        Returns:
            any: An object from
        """
        return self.syn.tableQuery(
            statement, includeRowIdAndRowVersion=include_row_data
        )

    def build_table(self, table_name: str, table: pd.DataFrame) -> None:
        """Adds a table to the project based on the input table

        Args:
            table_name (str): The name fo the table
            table (pd.DataFrame): A dataframe of the table
        """
        table_copy = table.copy(deep=False)
        project = self.syn.get(self.project_id)
        table_copy = sc.table.build_table(table_name, project, table_copy)
        self.syn.store(table_copy)

    def add_table(self, table_name: str, table_config: DBObjectConfig) -> None:
        """Adds a synapse table

        Args:
            table_name (str): The name of the table to be added
            table_config (DBObjectConfig): The config the table to be added
        """
        columns: list[sc.Column] = []
        values: dict[str, list] = {}
        for att in table_config.attributes:
            column = create_synapse_column(att.name, att.datatype)
            columns.append(column)
            values[att.name] = []

        schema = sc.Schema(name=table_name, columns=columns, parent=self.project_id)
        table = sc.Table(schema, values)
        table = self.syn.store(table)

    def delete_table(self, synapse_id: str) -> None:
        """Deletes a Synapse table
        Args:
            synapse_id (str): The Synapse id of the table to delete
        """
        self.syn.delete(synapse_id)

    def replace_table(self, table_name: str, table: pd.DataFrame) -> None:
        """
        Replaces synapse table with table made in table.
        The synapse id is preserved.
        Args:
            table_name (str): The name of the table to be replaced
            data (pd.DataFrame): A dataframe of the table to replace to old table with
        """
        if table_name not in self.get_table_names():
            self.build_table(table_name, table)
        else:
            synapse_id = self.get_synapse_id_from_table_name(table_name)

            self.delete_all_table_rows(synapse_id)
            self.delete_all_table_columns(synapse_id)
            self.add_table_columns2(synapse_id, table)
            self.insert_table_rows(synapse_id, table)

    def insert_table_rows(self, synapse_id: str, data: pd.DataFrame) -> None:
        """Insert table rows into Synapse table
        Args:
            synapse_id (str): The Synapse id of the table to add rows into
            data (pd.DataFrame): The rows to be added.
        """
        table = self.syn.get(synapse_id)
        self.syn.store(sc.Table(table, data))

    def upsert_table_rows(self, synapse_id: str, data: pd.DataFrame) -> None:
        """Upserts rows from  the given table

        Args:
            synapse_id (str): The Synapse ID fo the table to be upserted into
            data (pd.DataFrame): The table the rows will come from
        """
        self.syn.store(sc.Table(synapse_id, data))

    def delete_table_rows(self, synapse_id: str, data: pd.DataFrame) -> None:
        """Deletes rows from the given table
        Args:
            synapse_id (str): The Synapse id of the table the rows will be deleted from
            data (pd.DataFrame): A pandas.DataFrame. Columns must include "ROW_ID",
             and "ROW_VERSION"
        """
        columns = list(data.columns)
        if "ROW_ID" not in columns:
            raise SynapseDeleteRowsError(
                "ROW_ID missing from input data", synapse_id, columns
            )
        if "ROW_VERSION" not in columns:
            raise SynapseDeleteRowsError(
                "ROW_VERSION missing from input data", synapse_id, columns
            )
        self.syn.delete(sc.Table(synapse_id, data))

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_fixed(1),
        retry=retry_if_exception_type(sc.core.exceptions.SynapseHTTPError),
    )
    def delete_all_table_rows(self, synapse_id: str) -> None:
        """Deletes all rows in the Synapse table

        Args:
            synapse_id (str): The Synapse id of the table
        """
        table = self.syn.get(synapse_id)
        columns = self.syn.getTableColumns(table)
        if len(list(columns)) > 0:
            results = self.syn.tableQuery(f"select * from {synapse_id}")
            self.syn.delete(results)

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_fixed(1),
        retry=retry_if_exception_type(sc.core.exceptions.SynapseHTTPError),
    )
    def delete_all_table_columns(self, synapse_id: str) -> None:
        """Deletes all columns in the Synapse table

        Args:
            synapse_id (str): The Synapse id of the table
        """
        table = self.syn.get(synapse_id)
        columns = self.syn.getTableColumns(table)
        for col in columns:
            table.removeColumn(col)
        self.syn.store(table)

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_fixed(1),
        retry=retry_if_exception_type(sc.core.exceptions.SynapseHTTPError),
    )
    def add_table_columns(self, synapse_id: str, table_config: DBObjectConfig) -> None:
        """Add columns to synapse table from config

        Args:
            synapse_id (str): The Synapse id of the table to add the columns to
            table_config (pd.DataFrame): The config for the table to add the columns to
        """
        new_columns = [
            create_synapse_column(att.name, att.datatype)
            for att in table_config.attributes
        ]

        table = self.syn.get(synapse_id)
        for col in new_columns:
            table.addColumn(col)
        self.syn.store(table)

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_fixed(1),
        retry=retry_if_exception_type(sc.core.exceptions.SynapseHTTPError),
    )
    def add_table_columns2(self, synapse_id: str, data: pd.DataFrame) -> None:
        """Add columns to synapse table from dataframe

        Args:
            synapse_id (str): The Synapse id of the table to add the columns to
            data (pd.DataFrame): The dataframe the columns will be made from
        """
        current_table = self.syn.get(synapse_id)
        new_columns = sc.as_table_columns(data)
        for col in new_columns:
            current_table.addColumn(col)
        self.syn.store(current_table)

    def get_entity_annotations(self, synapse_id: str) -> sc.Annotations:
        """Gets the annotations for the Synapse entity

        Args:
            synapse_id (str): The Synapse id of the entity

        Returns:
            synapseclient.Annotations: The annotations of the Synapse entity in dict form.
        """
        return self.syn.get_annotations(synapse_id)

    def set_entity_annotations(
        self, synapse_id: str, annotations: dict[str, Any]
    ) -> None:
        """Sets the entities annotations to the input annotations

        Args:
            synapse_id (str): The Synapse ID of the entity
            annotations (dict[str, Any]): A dictionary of annotations
        """
        entity_annotations = self.syn.get_annotations(synapse_id)
        entity_annotations.clear()
        for key, value in annotations.items():
            entity_annotations[key] = value
        self.syn.set_annotations(entity_annotations)

    def clear_entity_annotations(self, synapse_id: str) -> None:
        """Removes all annotations from the entity

        Args:
            synapse_id (str): The Synapse ID of the entity
        """
        annotations = self.syn.get_annotations(synapse_id)
        annotations.clear()
        self.syn.set_annotations(annotations)
