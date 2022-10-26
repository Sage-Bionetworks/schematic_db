"""SynapseDatabase"""
import pandas as pd
from synapseclient.core.exceptions import SynapseHTTPError  # type: ignore
from schematic_db.db_config import DBObjectConfig
from schematic_db.synapse import Synapse
from .rdb import RelationalDatabase, UpdateDBTableError


class SynapseDatabase(RelationalDatabase):
    """Represents a database stored as Synapse tables"""

    def __init__(self, config_dict: dict):
        """Init
        Args:
            config_dict (dict): A dict with synapse specific fields
        """
        self.synapse = Synapse(config_dict)

    def execute_sql_query(
        self, query: str, include_row_data: bool = False
    ) -> pd.DataFrame:
        return self.synapse.execute_sql_query(query, include_row_data)

    def update_table(
        self,
        data: pd.DataFrame,
        table_config: DBObjectConfig,
        replace_table: bool = False,
    ) -> None:
        table_names = self.synapse.get_table_names()
        table_name = table_config.name
        if replace_table:
            self.synapse.replace_table(table_name, data)
            return
        if table_name not in table_names:
            self.synapse.add_table(table_name, table_config)
        try:
            self.synapse.upsert_table_rows(table_name, data, table_config)
        except SynapseHTTPError as error:
            table_id = self.synapse.get_synapse_id_from_table_name(table_name)
            error_msg = f"Synapse error; synapse id: {table_id}"
            raise UpdateDBTableError(table_name, error_msg) from error

    def drop_table(self, table_name: str) -> None:
        self.synapse.drop_table(table_name)

    def delete_table_rows(
        self, table_name: str, data: pd.DataFrame, table_config: DBObjectConfig
    ) -> None:
        pass
