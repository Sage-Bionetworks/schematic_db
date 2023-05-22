"""SynapseDatabase"""
from typing import Union
from functools import partial
import pandas as pd
import synapseclient as sc  # type: ignore
from schematic_db.db_schema.db_schema import (
    DatabaseSchema,
    TableSchema,
    ForeignKeySchema,
    ColumnSchema,
    ColumnDatatype,
)
from schematic_db.synapse.synapse import Synapse, SynapseConfig
from .rdb import RelationalDatabase

CONFIG_DATATYPES = {
    "text": ColumnDatatype.TEXT,
    "date": ColumnDatatype.DATE,
    "int": ColumnDatatype.INT,
    "float": ColumnDatatype.FLOAT,
    "boolean": ColumnDatatype.BOOLEAN,
}


class SynapseDatabaseMissingTableAnnotationsError(Exception):
    """Raised when a table is missing expected annotations"""

    def __init__(self, message: str, table_name: str) -> None:
        self.message = message
        self.table_name = table_name
        super().__init__(self.message)

    def __str__(self) -> str:
        return f"{self.message}; " f"name: {self.table_name};"


class SynapseDatabaseDropTableError(Exception):
    """SynapseDatabaseDropTableError"""

    def __init__(
        self, message: str, table_name: str, reverse_dependencies: list[str]
    ) -> None:
        self.message = message
        self.table_name = table_name
        self.reverse_dependencies = reverse_dependencies
        super().__init__(self.message)

    def __str__(self) -> str:
        return (
            f"{self.message}; "
            f"name: {self.table_name}; "
            f"reverse_dependencies: {self.reverse_dependencies}"
        )


class SynapseDatabaseUpdateTableError(Exception):
    """SynapseDatabaseDropTableError"""

    def __init__(
        self, table_name: str, foreign_key: str, values: list[str], dependency: str
    ) -> None:
        self.message = "Error updating table"
        self.table_name = table_name
        self.foreign_key = foreign_key
        self.values = values
        self.dependency = dependency
        super().__init__(self.message)

    def __str__(self) -> str:
        return (
            f"{self.message}; "
            f"name: {self.table_name}; "
            f"foreign key: {self.foreign_key}; "
            f"values: {self.values}; "
            f"one or more values missing in dependency: {self.dependency}; "
        )


def create_foreign_key_annotation_string(key: ForeignKeySchema) -> str:
    """Creates a string that will serve as a foreign key Synapse annotation

    Args:
        key (ForeignKeySchema): The foreign key to be turned into a string

    Returns:
        str: The foreign key in string form.
    """
    return f"{key.name};{key.foreign_table_name};{key.foreign_column_name}"


def create_attribute_annotation_string(column_schema: ColumnSchema) -> str:
    """Creates a string that will serve as a foreign key Synapse annotation

    Args:
        column_schema (ColumnSchema): The attribute to be turned into a string

    Returns:
        str: The attribute in string form.
    """
    return f"{column_schema.name};{column_schema.datatype.value};{str(column_schema.required)}"


def create_foreign_keys(strings: list[str]) -> list[ForeignKeySchema]:
    """Creates a list of ForeignKeySchemas from a list of Synapse table entity strings

    Args:
        strings (list[str]): A list of strings each representing a foreign key

    Returns:
        list[ForeignKeySchema]: A list of ForeignKeySchemas
    """
    if strings is None:
        return []
    lists: list[list[str]] = [key.split(";") for key in strings]
    return [
        ForeignKeySchema(
            name=key[0],
            foreign_table_name=key[1],
            foreign_column_name=key[2],
        )
        for key in lists
    ]


def create_column_schemas(column_annotations: list[str]) -> list[ColumnSchema]:
    """Creates a list of ColumnSchemas from a list of Synapse table entity strings

    Args:
        column_annotations (list[str]): A list of strings each representing an column

    Returns:
        list[ColumnSchema]:  A list of ColumnSchemas
    """
    column_lists = [att.split(";") for att in column_annotations]
    return [
        ColumnSchema(
            name=att[0], datatype=CONFIG_DATATYPES[att[1]], required=att[2] == "True"
        )
        for att in column_lists
    ]


def create_synapse_column(name: str, datatype: ColumnDatatype) -> sc.Column:
    """Creates a Synapse column object

    Args:
        name (str): The name of the column
        datatype (ColumnDatatype): The datatype of the column

    Returns:
        sc.Column: _description_
    """
    datatypes = {
        ColumnDatatype.TEXT: partial(sc.Column, columnType="LARGETEXT"),
        ColumnDatatype.DATE: partial(sc.Column, columnType="DATE"),
        ColumnDatatype.INT: partial(sc.Column, columnType="INTEGER"),
        ColumnDatatype.FLOAT: partial(sc.Column, columnType="DOUBLE"),
        ColumnDatatype.BOOLEAN: partial(sc.Column, columnType="BOOLEAN"),
    }
    func = datatypes[datatype]
    return func(name=name)


class SynapseDatabase(RelationalDatabase):
    """Represents a database stored as Synapse tables"""

    def __init__(self, config: SynapseConfig):
        """Init
        Args:
            config (SynapseConfig): A SynapseConfig object
        """
        self.synapse = Synapse(config)

    def query_table(self, table_name: str) -> pd.DataFrame:
        synapse_id = self.synapse.get_synapse_id_from_table_name(table_name)
        table = self.synapse.query_table(synapse_id)
        return table

    def drop_all_tables(self) -> None:
        database_schema = self.get_database_schema()
        deps = {
            table: database_schema.get_dependencies(table)
            for table in database_schema.get_schema_names()
        }
        tables_with_no_deps = [key for key, value in deps.items() if value == []]
        for table in tables_with_no_deps:
            self._drop_table_and_dependencies(table, database_schema)

    def drop_table_and_dependencies(self, table_name: str) -> None:
        """Drops the table and any tables that depend on it.

        Args:
            table_name (str): The name of the table
        """
        db_schema = self.get_database_schema()
        self._drop_table_and_dependencies(table_name, db_schema)

    def _drop_table_and_dependencies(
        self, table_name: str, db_schema: DatabaseSchema
    ) -> None:
        """Drops the table and any tables that depend on it.

        Args:
            table_name (str): The name of the table
            db_schema (DatabaseSchema): The configuration for the database
        """
        self._drop_all_table_dependencies(table_name, db_schema)
        table_id = self.synapse.get_synapse_id_from_table_name(table_name)
        self._drop_table(table_id)

    def _drop_all_table_dependencies(
        self, table_name: str, db_schema: DatabaseSchema
    ) -> None:
        """Drops all tables that depend on the input table

        Args:
            table_name (str): The name of the table whose dependent table will be dropped
            db_schema (DatabaseSchema): The configuration of the database
        """
        reverse_dependencies = db_schema.get_reverse_dependencies(table_name)
        for rd_table_name in reverse_dependencies:
            self._drop_table_and_dependencies(rd_table_name, db_schema)

    def delete_all_tables(self) -> None:
        """Deletes all tables in the project"""
        table_names = self.get_table_names()
        for name in table_names:
            self.delete_table(name)

    def delete_table(self, table_name: str) -> None:
        """Deletes the table entity

        Args:
            table_name (str): The name of the table to delete
        """
        table_id = self.synapse.get_synapse_id_from_table_name(table_name)
        self.synapse.delete_table(table_id)

    def drop_table(self, table_name: str) -> None:
        database_schema = self.get_database_schema()
        reverse_dependencies = database_schema.get_reverse_dependencies(table_name)
        if len(reverse_dependencies) != 0:
            raise SynapseDatabaseDropTableError(
                "Can not drop database table, other tables exists that depend on it.",
                table_name,
                reverse_dependencies,
            )

        table_id = self.synapse.get_synapse_id_from_table_name(table_name)
        self._drop_table(table_id)

    def _drop_table(self, table_id: str) -> None:
        self.synapse.delete_all_table_rows(table_id)
        self.synapse.delete_all_table_columns(table_id)
        self.synapse.clear_entity_annotations(table_id)

    def execute_sql_query(
        self, query: str, include_row_data: bool = False
    ) -> pd.DataFrame:
        return self.synapse.execute_sql_query(query, include_row_data)

    def check_dependencies(self, data: pd.DataFrame, table_config: TableSchema) -> None:
        """Checks if the dataframe's foreign keys are in the tables dependencies

        Args:
            data (pd.DataFrame): The dataframe to be inserted
            table_config (TableSchema): The config of the table to be inserted into

        Raises:
            SynapseDatabaseUpdateTableError: Raised when there are values in foreign key columns
             that don't exist in the tables dependencies
        """
        for key in table_config.foreign_keys:
            if key.name not in data.columns:
                continue
            insert_keys = [key for key in data[key.name].tolist() if not pd.isnull(key)]
            table_id = self.synapse.get_synapse_id_from_table_name(
                key.foreign_table_name
            )
            table = self._create_primary_key_table(table_id, key.foreign_column_name)
            current_keys = table[key.foreign_column_name].tolist()
            if not set(insert_keys).issubset(current_keys):
                raise SynapseDatabaseUpdateTableError(
                    table_name=table_config.name,
                    foreign_key=key.name,
                    values=insert_keys,
                    dependency=key.foreign_table_name,
                )

    def add_table(self, table_name: str, table_schema: TableSchema) -> None:
        table_names = self.synapse.get_table_names()
        table_name = table_schema.name
        columns = [
            create_synapse_column(att.name, att.datatype)
            for att in table_schema.columns
        ]

        if table_name not in table_names:
            self.synapse.add_table(table_name, columns)
        else:
            synapse_id = self.synapse.get_synapse_id_from_table_name(table_name)
            self.synapse.add_table_columns(synapse_id, columns)

        self.annotate_table(table_name, table_schema)

    def get_table_names(self) -> list[str]:
        return self.synapse.get_table_names()

    def annotate_table(self, table_name: str, table_schema: TableSchema) -> None:
        """Annotates the table with it's primary key and foreign keys

        Args:
            table_name (str): The name of the table to be annotated
            table_schema (TableSchema): The config for the table
        """
        synapse_id = self.synapse.get_synapse_id_from_table_name(table_name)
        annotations: dict[str, Union[str, list[str]]] = {
            f"attribute{str(i)}": create_attribute_annotation_string(att)
            for i, att in enumerate(table_schema.columns)
        }
        annotations["primary_key"] = table_schema.primary_key
        if len(table_schema.foreign_keys) > 0:
            foreign_key_strings = [
                create_foreign_key_annotation_string(key)
                for key in table_schema.foreign_keys
            ]
            annotations["foreign_keys"] = foreign_key_strings
        self.synapse.set_entity_annotations(synapse_id, annotations)

    def get_database_schema(self) -> DatabaseSchema:
        """Gets the schema of the synapse database.

        Returns:
            DatabaseSchema: The db schema
        """
        table_names = self.synapse.get_table_names()
        result_list = [self.get_table_schema(name) for name in table_names]
        schema_list = [schema for schema in result_list if schema is not None]
        return DatabaseSchema(schema_list)

    def get_table_schema(self, table_name: str) -> TableSchema:
        """Creates a TableSchema if the table is annotated, otherwise None

        Args:
            table_name (str): The name of the table

        Raises:
            SynapseDatabaseMissingTableAnnotationsError: Raised when the table ahs no annotations

        Returns:
            TableSchema: A generic representation of the table
        """
        table_id = self.synapse.get_synapse_id_from_table_name(table_name)
        annotations = self.synapse.get_entity_annotations(table_id)
        # if a synapse table has been "dropped" but not deleted or rebuilt
        if not annotations:
            raise SynapseDatabaseMissingTableAnnotationsError(
                "Table has no annotations", table_name
            )
        column_annotations = [
            v[0] for k, v in annotations.items() if k.startswith("attribute")
        ]
        return TableSchema(
            name=table_name,
            primary_key=annotations["primary_key"][0],
            foreign_keys=create_foreign_keys(annotations.get("foreign_keys")),
            columns=create_column_schemas(column_annotations),
        )

    def delete_table_rows(self, table_name: str, data: pd.DataFrame) -> None:
        database_schema = self.get_database_schema()
        primary_key = database_schema.get_schema_by_name(table_name).primary_key
        table_id = self.synapse.get_synapse_id_from_table_name(table_name)
        merged_data = self._merge_dataframe_with_primary_key_table(
            table_id, data, primary_key
        )
        self._delete_table_rows(table_name, table_id, merged_data, database_schema)

    def _delete_table_rows(
        self,
        table_name: str,
        table_id: str,
        data: pd.DataFrame,
        database_schema: DatabaseSchema,
    ) -> None:
        """Deletes rows from the given table

        Args:
            table_name (str): The name of the table the rows will be deleted from
            table_id (str): The id of the table the rows will be deleted from
            data (pd.DataFrame): A pandas.DataFrame, of just it's primary key, ROW_ID, and
             ROW_VERSION
            database_schema (DatabaseSchema): The schema for the database
        """
        primary_key = database_schema.get_schema_by_name(table_name).primary_key
        self._delete_table_dependency_rows(
            table_name, database_schema, data[[primary_key]]
        )
        self.synapse.delete_table_rows(table_id, data)

    def _delete_table_dependency_rows(
        self,
        table_name: str,
        database_schema: DatabaseSchema,
        data: pd.DataFrame,
    ) -> None:
        """Deletes rows from the tables that are dependant on the given table

        Args:
            table_name (str): The name of the table whose reverse dependencies will have their rows
             deleted from
            database_schema (DatabaseSchema): The schema for the database
            data (pd.DataFrame): A pandas.DataFrame, of just it's primary key.
        """
        reverse_dependencies = database_schema.get_reverse_dependencies(table_name)
        for rd_table_name in reverse_dependencies:
            # gathering data about the reverse dependency
            table_id = self.synapse.get_synapse_id_from_table_name(rd_table_name)
            primary_key = database_schema.get_schema_by_name(rd_table_name).primary_key
            foreign_keys = database_schema.get_schema_by_name(
                rd_table_name
            ).foreign_keys
            foreign_key = [
                key for key in foreign_keys if key.foreign_table_name == table_name
            ][0]

            # get the reverse dependency data with just its primary and foreign key
            query = f"SELECT {primary_key}, {foreign_key.name} FROM {table_id}"
            rd_data = self.execute_sql_query(query, include_row_data=True)

            # merge the reverse dependency data with the input data
            data = pd.merge(
                rd_data,
                data,
                how="inner",
                left_on=foreign_key.name,
                right_on=foreign_key.foreign_column_name,
            )

            # if data has no rows continue to next reverse dependency
            if len(data.index) == 0:
                continue

            data = data[[primary_key, "ROW_ID", "ROW_VERSION"]]
            self._delete_table_rows(rd_table_name, table_id, data, database_schema)

    def insert_table_rows(self, table_name: str, data: pd.DataFrame) -> None:
        """Inserts rows into the given table

        Args:
            table_name (str): The name of the table the rows be upserted into
            data (pd.DataFrame): A pandas.DataFrame. It must contain the primary keys of the table
        """
        table_id = self.synapse.get_synapse_id_from_table_name(table_name)
        self.synapse.insert_table_rows(table_id, data)

    def upsert_table_rows(self, table_name: str, data: pd.DataFrame) -> None:
        """Upserts rows into the given table

        Args:
            table_name (str): The name of the table to be upserted into.
            data (pd.DataFrame): The table the rows will come from

        Raises:
            SynapseDatabaseMissingTableAnnotationsError: Raised when the table has no
             primary key annotation.
        """
        table_id = self.synapse.get_synapse_id_from_table_name(table_name)
        annotations = self.synapse.get_entity_annotations(table_id)
        if "primary_key" not in annotations:
            raise SynapseDatabaseMissingTableAnnotationsError(
                "Table has no primary_key annotation", table_name
            )
        primary_key = annotations["primary_key"][0]
        self._upsert_table_rows(table_id, data, primary_key)

    def _upsert_table_rows(
        self, table_id: str, data: pd.DataFrame, primary_key: str
    ) -> None:
        """Upserts rows into the given table

        Args:
            table_id (str): The Synapse id of the table to be upserted into.
            data (pd.DataFrame): The table the rows will come from
            primary_key (str): The primary key of the table used to identify which rows to update
        """
        table = self._create_primary_key_table(table_id, primary_key)
        merged_table = pd.merge(data, table, how="left", on=primary_key)
        self.synapse.upsert_table_rows(table_id, merged_table)

    def _merge_dataframe_with_primary_key_table(
        self, table_id: str, data: pd.DataFrame, primary_key: str
    ) -> pd.DataFrame:
        """
        Merges the dataframe with a table that has just the primary key column.
        This is used to filter the table to only have rows where the primary key
         currently exists in the database.

        Args:
            table_id (str): The id of the table to query
            data (pd.DataFrame): The dataframe to merge with the primary key
            primary_key (str): The name of the primary key

        Returns:
            pd.DataFrame: A dataframe with only rows where the primary key currently exists
        """
        data = data[[primary_key]]
        table = self.synapse.query_table(table_id, include_row_data=True)
        table = table[["ROW_ID", "ROW_VERSION", primary_key]]
        merged_table = pd.merge(data, table, how="inner", on=primary_key)
        return merged_table

    def _create_primary_key_table(
        self, table_id: str, primary_key: str
    ) -> pd.DataFrame:
        """Creates a dataframe with just the primary key of the table

        Args:
            table_id (str): The id of the table to query
            primary_key (str): The name of the primary key

        Returns:
            pd.DataFrame: The table in pandas.DataFrame form with the primary key, ROW_ID, and
             ROW_VERSION columns
        """
        table = self.synapse.query_table(table_id, include_row_data=True)
        table = table[["ROW_ID", "ROW_VERSION", primary_key]]
        return table
