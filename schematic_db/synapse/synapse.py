"""Synapse
"""
import time
from functools import partial
import typing
import synapseclient as sc  # type: ignore
import pandas as pd  # type: ignore
from schematic_db.db_config import DBObjectConfig, DBDatatype


SYNAPSE_DATATYPES = {
    DBDatatype.TEXT: partial(sc.Column, columnType="STRING", maximumSize=100),
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


class Synapse:
    """
    The Synapse class handles interactions with a project in Synapse.
    """

    def __init__(self, config_dict: dict) -> None:
        """Init

        Args:
            config_dict (dict): A dict with fields ["username", "auth_token", "project_id"]
        """
        username = config_dict.get("username")
        auth_token = config_dict.get("auth_token")
        project_id = config_dict.get("project_id")

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

    def execute_sql_statement(
        self, statement: str, include_row_data: bool = False
    ) -> typing.Any:
        """Execute a SQL statement

        Args:
            statement (str): A SQL statement that can be run by Synapse
            include_row_data (bool, optional): Include row_id and row_etag. Defaults to False.

        Returns:
            any: An object from
        """
        return self.syn.tableQuery(
            statement, includeRowIdAndRowVersion=include_row_data
        )

    def _get_tables(self) -> list[sc.Table]:
        project = self.syn.get(self.project_id)
        return list(self.syn.getChildren(project, includeTypes=["table"]))

    def query_table(
        self, table_name: str, table_config: DBObjectConfig
    ) -> pd.DataFrame:
        """Queries a whole table

        Args:
            table_name (str): The name of the table to query
            table_config (DBObjectConfig): The config for the table

        Returns:
            pd.DataFrame: The queried table
        """
        table_id = self.get_synapse_id_from_table_name(table_name)
        query = f"SELECT * FROM {table_id}"
        table = self.execute_sql_query(query)
        for att in table_config.attributes:
            if att.datatype == DBDatatype.INT:
                table = table.astype({att.name: "Int64"})
            elif att.datatype == DBDatatype.DATE:
                table[att.name] = pd.to_datetime(table[att.name], unit="ms").dt.date
            elif att.datatype == DBDatatype.BOOLEAN:
                table = table.astype({att.name: "boolean"})
        return table

    def execute_sql_query(
        self, query: str, include_row_data: bool = False
    ) -> pd.DataFrame:
        """Execute a Sql query

        Args:
            query (str): A SQL statement that can be run by Synapse
            include_row_data (bool, optional): Include row_id and row_etag. Defaults to False.

        Returns:
            pd.DataFrame: The queried table
        """
        result = self.execute_sql_statement(query, include_row_data)
        table = pd.read_csv(result.filepath)
        return table

    def add_table(self, table_name: str, table_config: DBObjectConfig) -> None:
        """Adds a synapse table

        Args:
            table_name (str): The name of the table to be added
            table_config (DBObjectConfig): The config the table to be added
        """
        columns: list[sc.Column] = []
        values: dict[str, list] = {}
        for att in table_config.attributes:
            column = self._create_synapse_column(att.name, att.datatype)
            columns.append(column)
            values[att.name] = []

        schema = sc.Schema(name=table_name, columns=columns, parent=self.project_id)
        table = sc.Table(schema, values)
        table = self.syn.store(table)

    def drop_table(self, table_name: str) -> None:
        """Drops a Synapse table

        Args:
            table_name (str): The name of the table to be dropped
        """
        synapse_id = self.get_synapse_id_from_table_name(table_name)
        self.syn.delete(synapse_id)

    def insert_table_rows(self, table_name: str, data: pd.DataFrame) -> None:
        """Insert table rows
        Args:
            table_name (str): The name of the table to add rows into
            data (pd.DataFrame): The rows to be added.
        """
        synapse_id = self.get_synapse_id_from_table_name(table_name)
        table = self.syn.get(synapse_id)
        self.syn.store(sc.Table(table, data))

    def delete_table_rows(
        self, table_name: str, data: pd.DataFrame, table_config: DBObjectConfig
    ) -> None:
        """Deletes rows from the given table
        Args:
            table_name (str): The name of the table the rows will be deleted from
            data (pd.DataFrame): A pandas.DataFrame. It must contain the primary keys of the table
            table_config (DBObjectConfig): A generic representation of the table as a
                DBObjectConfig object.
        """
        table_id = self.get_synapse_id_from_table_name(table_name)
        merged_table = self._merge_dataframe_with_primary_key_table(
            table_name, data, table_config
        )
        self.syn.delete(sc.Table(table_id, merged_table))

    def update_table_rows(
        self, table_name: str, data: pd.DataFrame, table_config: DBObjectConfig
    ) -> None:
        """Updates rows from the given table
        Args:
            table_name (str): The name of the table to be updated
            data (pd.DataFrame): A pandas.DataFrame. It must contain the primary keys of the table
            table_config (DBObjectConfig): A generic representation of the table as a
                DBObjectConfig object.
        """
        table_id = self.get_synapse_id_from_table_name(table_name)
        merged_table = self._merge_dataframe_with_primary_key_table(
            table_name, data, table_config
        )
        self.syn.store(sc.Table(table_id, merged_table))

    def upsert_table_rows(
        self, table_name: str, data: pd.DataFrame, table_config: DBObjectConfig
    ) -> None:
        """Upserts rows from  the given table

        Args:
            table_name (str): The name fo the table to be upserted into
            data (pd.DataFrame): The table the rows will come from
            table_config (DBObjectConfig): A generic representation of the table as a
                DBObjectConfig object.
        """
        table_id = self.get_synapse_id_from_table_name(table_name)
        primary_keys = table_config.primary_keys
        table = self._get_primary_key_table(table_name, primary_keys)
        merged_table = pd.merge(data, table, how="left", on=primary_keys)
        self.syn.store(sc.Table(table_id, merged_table))

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

            # deletes all current rows
            results = self.syn.tableQuery(f"select * from {synapse_id}")
            self.syn.delete(results)

            # wait for Synapse to catch up
            time.sleep(1)

            # removes all current columns
            current_table = self.syn.get(synapse_id)
            current_columns = self.syn.getTableColumns(current_table)
            for col in current_columns:
                current_table.removeColumn(col)

            # adds new columns to schema
            new_columns = sc.as_table_columns(table)
            for col in new_columns:
                current_table.addColumn(col)
            self.syn.store(current_table)

            # inserts new rows
            self.insert_table_rows(table_name, table)

    def build_table(self, table_name: str, table: pd.DataFrame) -> None:
        """Adds a table to the project based on the input table

        Args:
            table_name (str): The name fo the table
            table (pd.DataFrame): A dataframe of the table
        """
        project = self.syn.get(self.project_id)
        table = sc.table.build_table(table_name, project, table)
        self.syn.store(table)

    def read_csv_file(self, synapse_id: str) -> pd.DataFrame:
        """Gets a csv file in synapse and returns a dataframe

        Args:
            synapse_id (str): The synapse id of the csv

        Returns:
            pd.DataFrame: the csv in pandas.Dataframe form
        """
        path = self.syn.get(synapse_id).path
        return pd.read_csv(path)

    def _merge_dataframe_with_primary_key_table(
        self, table_name: str, data: pd.DataFrame, table_config: DBObjectConfig
    ) -> pd.DataFrame:
        primary_keys = table_config.primary_keys
        table = self._get_primary_key_table(table_name, primary_keys)
        merged_table = pd.merge(data, table, how="inner", on=primary_keys)
        return merged_table

    def _get_primary_key_table(
        self, table_name: str, primary_keys: list[str]
    ) -> pd.DataFrame:
        primary_key_string = ",".join(primary_keys)
        table_id = self.get_synapse_id_from_table_name(table_name)
        query = f"SELECT {primary_key_string} FROM {table_id}"
        table = self.execute_sql_query(query, include_row_data=True)
        return table

    def _create_synapse_column(self, name: str, datatype: DBDatatype) -> sc.Column:
        func = SYNAPSE_DATATYPES[datatype]
        return func(name=name)
