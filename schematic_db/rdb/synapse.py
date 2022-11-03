"""SynapseDatabase"""
from typing import Union
import pandas as pd
from schematic_db.db_config import (
    DBConfig,
    DBObjectConfig,
    DBForeignKey,
    DBAttributeConfig,
    DBDatatype,
)
from schematic_db.synapse import Synapse, SynapseConfig
from .rdb import RelationalDatabase

CONFIG_DATATYPES = {
    "text": DBDatatype.TEXT,
    "date": DBDatatype.DATE,
    "int": DBDatatype.INT,
    "float": DBDatatype.FLOAT,
    "boolean": DBDatatype.BOOLEAN,
}


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
    return f"{attribute.name};{attribute.datatype.value}"


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
        DBAttributeConfig(name=att[0], datatype=CONFIG_DATATYPES[att[1]])
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

    def drop_all_tables(self) -> None:
        table_names = self.synapse.get_table_names()
        for name in table_names:
            self.drop_table(name)

    def execute_sql_query(
        self, query: str, include_row_data: bool = False
    ) -> pd.DataFrame:
        return self.synapse.execute_sql_query(query, include_row_data)

    def update_table(self, data: pd.DataFrame, table_config: DBObjectConfig) -> None:
        table_names = self.synapse.get_table_names()
        table_name = table_config.name

        # table doesn't exist in Synapse, and must be built
        if table_name not in table_names:
            self.synapse.build_table(table_name, data)
            self.annotate_table(table_name, table_config)
            return

        # table exists but has no columns/rows, both must be added
        current_columns = self.synapse.get_table_column_names(table_name)
        if len(list(current_columns)) == 0:
            self.synapse.add_table_columns(table_name, data)
            self.synapse.insert_table_rows(table_name, data)
            self.annotate_table(table_name, table_config)
            return

        # table exists and possibly has data, upsert method must be used
        self.synapse.upsert_table_rows(table_name, data, table_config)

    def drop_table(self, table_name: str) -> None:
        synapse_id = self.synapse.get_synapse_id_from_table_name(table_name)
        self.synapse.delete_all_table_rows(synapse_id)
        self.synapse.delete_all_table_columns(synapse_id)
        self.synapse.clear_entity_annotations(synapse_id)

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
            "primary_key": table_config.primary_key,
            "attributes": [
                create_attribute_annotation_string(att)
                for att in table_config.attributes
            ],
        }
        if len(table_config.foreign_keys) > 0:
            foreign_key_strings = [
                create_foreign_key_annotation_string(key)
                for key in table_config.foreign_keys
            ]
            annotations["foreign_keys"] = foreign_key_strings
        self.synapse.set_entity_annotations(synapse_id, annotations)

    def get_db_config(self) -> DBConfig:
        """Gets all the table annotations

        Returns:
            dict[str, dict[str, Union[str, dict[str, str]]]]: A dict of table annotations
        """
        table_names = self.synapse.get_table_names()
        return DBConfig([self.get_table_config(name) for name in table_names])

    def get_table_config(self, table_name: str) -> DBObjectConfig:
        """Creates

        Args:
            table_name (str): The name fo the table

        Returns:
            DBObjectConfig: A generic representation of the table
        """
        table_id = self.synapse.get_synapse_id_from_table_name(table_name)
        annotations = self.synapse.get_entity_annotations(table_id)
        return DBObjectConfig(
            name=table_name,
            primary_key=annotations["primary_key"][0],
            foreign_keys=create_foreign_keys(annotations.get("foreign_keys")),
            attributes=create_attributes(annotations["attributes"]),
        )

    def delete_table_rows(
        self, table_name: str, data: pd.DataFrame, table_config: DBObjectConfig
    ) -> None:
        """Deletes rows from the given table

        Args:
            table_name (str): The name of the table the rows will be deleted from
            data (pd.DataFrame): A pandas.DataFrame. It must include the primary key of the table
            table_config (DBObjectConfig): A generic representation of the table as a
                DBObjectConfig object.
        """
        db_config = self.get_db_config()
        primary_key = db_config.get_config_by_name(table_name).primary_key
        data = data[[primary_key]]
        table_id = self.synapse.get_synapse_id_from_table_name(table_name)
        self._delete_table_rows(table_name, table_id, data, db_config)

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
            data (pd.DataFrame): A pandas.DataFrame, of just it's primary key.
            db_config (DBConfig): The configuration for the database
        """
        self._delete_table_dependency_rows(table_name, db_config, data)
        primary_key = db_config.get_config_by_name(table_name).primary_key
        merged_table = self._merge_dataframe_with_primary_key_table(
            table_id, data, primary_key
        )
        self.synapse.delete_table_rows(table_id, merged_table)

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
            rd_data = self.execute_sql_query(query)

            # merge the reverse dependency data with the input data
            data = pd.merge(
                rd_data,
                data,
                how="inner",
                left_on=foreign_key.name,
                right_on=foreign_key.foreign_attribute_name,
            )

            # reduce data to just its primary key
            data = data[[primary_key]]

            self._delete_table_rows(rd_table_name, table_id, data, db_config)

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
        table = self._create_primary_key_table(table_id, primary_key)
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
        query = f"SELECT {primary_key} FROM {table_id}"
        table = self.execute_sql_query(query, include_row_data=True)
        return table
