"""SynapseDatabase"""
from typing import Union, Optional
import pandas as pd
from schematic_db.db_config import (
    DBConfig,
    DBObjectConfig,
    DBForeignKey,
    DBAttributeConfig,
    DBDatatype,
)
from schematic_db.synapse import Synapse, SynapseConfig
from .rdb import RelationalDatabase, UpdateDBTableError

CONFIG_DATATYPES = {
    "text": DBDatatype.TEXT,
    "date": DBDatatype.DATE,
    "int": DBDatatype.INT,
    "float": DBDatatype.FLOAT,
    "boolean": DBDatatype.BOOLEAN,
}


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


def create_foreign_key_annotation_string(key: DBForeignKey) -> str:
    """Creates a string that will serve as a foreign key Synapse annotation

    Args:
        key (DBForeignKey): The foreign key to be turned into a string

    Returns:
        str: The foreign key in string form.
    """
    return f"{key.name};{key.foreign_object_name};{key.foreign_attribute_name}"


def create_attribute_annotation_string(attribute: DBAttributeConfig) -> str:
    """Creates a string that will serve as a foreign key Synapse annotation

    Args:
        key (DBAttributeConfig): The attribute to be turned into a string

    Returns:
        str: The attribute in string form.
    """
    return f"{attribute.name};{attribute.datatype.value};{str(attribute.required)}"


def create_foreign_keys(strings: list[str]) -> list[DBForeignKey]:
    """Creates a list of DBForeignKeys from a list of Synapse table entity strings

    Args:
        strings (list[str]): A list of strings each representing a foreign key

    Returns:
        list[DBForeignKey]: A list of DBForeignKeys
    """
    if strings is None:
        return []
    lists: list[list[str]] = [key.split(";") for key in strings]
    return [
        DBForeignKey(
            name=key[0],
            foreign_object_name=key[1],
            foreign_attribute_name=key[2],
        )
        for key in lists
    ]


def create_attributes(strings: list[str]) -> list[DBAttributeConfig]:
    """Creates a list of DBAttributeConfigs from a list of Synapse table entity strings

    Args:
        strings (list[str]): A list of strings each representing an attribute

    Returns:
        list[DBAttributeConfig]: A list of DBAttributeConfigs
    """
    attribute_lists = [att.split(";") for att in strings]
    return [
        DBAttributeConfig(
            name=att[0], datatype=CONFIG_DATATYPES[att[1]], required=bool(att[2])
        )
        for att in attribute_lists
    ]


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
        db_config = self.get_db_config()
        deps = {
            table: db_config.get_dependencies(table)
            for table in db_config.get_config_names()
        }
        tables_with_no_deps = [key for key, value in deps.items() if value == []]
        for table in tables_with_no_deps:
            self._drop_table_and_dependencies(table, db_config)

    def drop_table_and_dependencies(self, table_name: str) -> None:
        """Drops the table and any tables that depend on it.

        Args:
            table_name (str): The name of the table
        """
        db_config = self.get_db_config()
        self._drop_table_and_dependencies(table_name, db_config)

    def _drop_table_and_dependencies(
        self, table_name: str, db_config: DBConfig
    ) -> None:
        """Drops the table and any tables that depend on it.

        Args:
            table_name (str): The name of the table
            db_config (DBConfig): The configuration for the database
        """
        self._drop_all_table_dependencies(table_name, db_config)
        table_id = self.synapse.get_synapse_id_from_table_name(table_name)
        self._drop_table(table_id)

    def _drop_all_table_dependencies(
        self, table_name: str, db_config: DBConfig
    ) -> None:
        """Drops all tables that depend on the input table

        Args:
            table_name (str): The name of the table whose dependent table will be dropped
            db_config (DBConfig): The configuration fo the database
        """
        reverse_dependencies = db_config.get_reverse_dependencies(table_name)
        for rd_table_name in reverse_dependencies:
            self._drop_table_and_dependencies(rd_table_name, db_config)

    def delete_all_tables(self) -> None:
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
        db_config = self.get_db_config()
        reverse_dependencies = db_config.get_reverse_dependencies(table_name)
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

    def check_dependencies(
        self, data: pd.DataFrame, table_config: DBObjectConfig
    ) -> None:
        """Checks if the dataframe's foreign keys are in the tables dependencies

        Args:
            data (pd.DataFrame): The dataframe to be inserted
            table_config (DBObjectConfig): The config of the table to be inserted into

        Raises:
            SynapseDatabaseUpdateTableError: Raised when there are values in foreign key columns
             that don't exist in the tables dependencies
        """
        for key in table_config.foreign_keys:
            if key.name not in data.columns:
                continue
            insert_keys = [key for key in data[key.name].tolist() if not pd.isnull(key)]
            table_id = self.synapse.get_synapse_id_from_table_name(
                key.foreign_object_name
            )
            table = self._create_primary_key_table(table_id, key.foreign_attribute_name)
            current_keys = table[key.foreign_attribute_name].tolist()
            if not set(insert_keys).issubset(current_keys):
                raise SynapseDatabaseUpdateTableError(
                    table_name=table_config.name,
                    foreign_key=key.name,
                    values=insert_keys,
                    dependency=key.foreign_object_name,
                )

    def update_table(self, data: pd.DataFrame, table_config: DBObjectConfig) -> None:
        try:
            self.check_dependencies(data, table_config)
        except SynapseDatabaseUpdateTableError as error:
            raise UpdateDBTableError(table_config.name, str(error)) from error

        # synapse client can have some problems with <NA> values in string columns
        for attribute in table_config.attributes:
            if attribute.datatype == DBDatatype.TEXT and attribute.name in data.columns:
                data[attribute.name].fillna("", inplace=True)

        table_names = self.synapse.get_table_names()
        table_name = table_config.name

        # table doesn't exist in Synapse, and must be built
        if table_name not in table_names:
            self.synapse.add_table(table_name, table_config)
            synapse_id = self.synapse.get_synapse_id_from_table_name(table_name)
            self.annotate_table(table_name, table_config)
            self.synapse.insert_table_rows(synapse_id, data)
            return

        # table exists but has no columns/rows, both must be added
        current_columns = self.synapse.get_table_column_names(table_name)
        if len(list(current_columns)) == 0:
            synapse_id = self.synapse.get_synapse_id_from_table_name(table_name)
            self.annotate_table(table_name, table_config)
            self.synapse.add_table_columns(synapse_id, table_config)
            self.synapse.insert_table_rows(synapse_id, data)
            return

        # table exists and possibly has data, upsert method must be used
        self.upsert_table_rows(table_name, data)

    def get_table_names(self) -> list[str]:
        return self.synapse.get_table_names()

    def annotate_table(self, table_name: str, table_config: DBObjectConfig) -> None:
        """Annotates the table with it's primary key and foreign keys

        Args:
            table_name (str): The name of the table to be annotated
            table_config (DBObjectConfig): The config for the table
        """
        synapse_id = self.synapse.get_synapse_id_from_table_name(table_name)
        annotations: dict[str, Union[str, list[str]]] = {
            f"attribute{str(i)}": create_attribute_annotation_string(att)
            for i, att in enumerate(table_config.attributes)
        }
        annotations["primary_key"] = table_config.primary_key
        if len(table_config.foreign_keys) > 0:
            foreign_key_strings = [
                create_foreign_key_annotation_string(key)
                for key in table_config.foreign_keys
            ]
            annotations["foreign_keys"] = foreign_key_strings
        self.synapse.set_entity_annotations(synapse_id, annotations)

    def get_db_config(self) -> DBConfig:
        table_names = self.synapse.get_table_names()
        result_list = [self.get_table_config(name) for name in table_names]
        config_list = [config for config in result_list if config is not None]
        return DBConfig(config_list)

    def get_table_config(self, table_name: str) -> Optional[DBObjectConfig]:
        """Creates a DBObjectConfig if the table is annotated, otherwise None

        Args:
            table_name (str): The name fo the table

        Returns:
            DBObjectConfig: A generic representation of the table
        """
        table_id = self.synapse.get_synapse_id_from_table_name(table_name)
        annotations = self.synapse.get_entity_annotations(table_id)
        if not annotations:
            return None
        attribute_annotations = [
            v[0] for k, v in annotations.items() if k.startswith("attribute")
        ]
        return DBObjectConfig(
            name=table_name,
            primary_key=annotations["primary_key"][0],
            foreign_keys=create_foreign_keys(annotations.get("foreign_keys")),
            attributes=create_attributes(attribute_annotations),
        )

    def delete_table_rows(self, table_name: str, data: pd.DataFrame) -> None:
        db_config = self.get_db_config()
        primary_key = db_config.get_config_by_name(table_name).primary_key
        table_id = self.synapse.get_synapse_id_from_table_name(table_name)
        merged_data = self._merge_dataframe_with_primary_key_table(
            table_id, data, primary_key
        )
        self._delete_table_rows(table_name, table_id, merged_data, db_config)

    def _delete_table_rows(
        self,
        table_name: str,
        table_id: str,
        data: pd.DataFrame,
        db_config: DBConfig,
    ) -> None:
        """Deletes rows from the given table

        Args:
            table_name (str): The name of the table the rows will be deleted from
            table_id (str): The id of the table the rows will be deleted from
            data (pd.DataFrame): A pandas.DataFrame, of just it's primary key, ROW_ID, and
             ROW_VERSION
            db_config (DBConfig): The configuration for the database
        """
        primary_key = db_config.get_config_by_name(table_name).primary_key
        self._delete_table_dependency_rows(table_name, db_config, data[[primary_key]])
        self.synapse.delete_table_rows(table_id, data)

    def _delete_table_dependency_rows(
        self,
        table_name: str,
        db_config: DBConfig,
        data: pd.DataFrame,
    ) -> None:
        """Deletes rows from the tables that are dependant on the given table

        Args:
            table_name (str): The name of the table whose reverse dependencies will have their rows
             deleted from
            db_config (DBConfig): The configuration for the database
            data (pd.DataFrame): A pandas.DataFrame, of just it's primary key.
        """
        reverse_dependencies = db_config.get_reverse_dependencies(table_name)
        for rd_table_name in reverse_dependencies:
            # gathering data about the reverse dependency
            table_id = self.synapse.get_synapse_id_from_table_name(rd_table_name)
            primary_key = db_config.get_config_by_name(rd_table_name).primary_key
            foreign_keys = db_config.get_config_by_name(rd_table_name).foreign_keys
            foreign_key = [
                key for key in foreign_keys if key.foreign_object_name == table_name
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
                right_on=foreign_key.foreign_attribute_name,
            )

            # if data has no rows continue to next reverse dependency
            if len(data.index) == 0:
                continue

            data = data[[primary_key, "ROW_ID", "ROW_VERSION"]]
            self._delete_table_rows(rd_table_name, table_id, data, db_config)

    def upsert_table_rows(self, table_name: str, data: pd.DataFrame) -> None:
        """Upserts rows into the given table

        Args:
            table_name (str): The name of the table to be upserted into.
            data (pd.DataFrame): The table the rows will come from
        """
        table_id = self.synapse.get_synapse_id_from_table_name(table_name)
        primary_key = self.synapse.get_entity_annotations(table_id)["primary_key"][0]
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
