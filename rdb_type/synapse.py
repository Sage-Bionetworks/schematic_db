"""Synapse
"""
from typing import List
import synapseclient as sc
import pandas as pd
from db_object_config import DBObjectConfig, DBDatatype
from .rdb_type import RDBType

class Synapse(RDBType):
    """Synapse
    - Represents:
      - A database stored as Synapse tables
      - A source of manifest tables in Synapse
      - A destination of queries in Synapse
    - Implements the RDBType interface.
    - Handles Synapse specific functionality.
    """
    def __init__(self, config_dict: dict):
        """Init
        obj = MySQL({
            username: "firstname.lastname@sagebase.org"
            auth_token: "xxx"
            project_id: "syn1"
        })

        Args:
            config_dict (dict): A dict with synapse specific fields
        """
        username = config_dict.get("username")
        auth_token = config_dict.get("auth_token")
        project_id = config_dict.get("project_id")

        syn = sc.Synapse()
        syn.login(username, authToken=auth_token)

        self.syn = syn
        self.project_id = project_id

    def get_table_names(self) -> List[str]:
        tables = self._get_tables()
        return [table['name'] for table in tables]

    def get_table_id_from_name(self, table_name: str) -> str:
        tables = self._get_tables()
        matching_tables = [table for table in tables if table['name'] == table_name]
        if len(matching_tables) == 0:
            raise ValueError(f"No matching tables with name {table_name}")
        if len(matching_tables) > 1:
            raise ValueError(f"Multiple matching tables with name {table_name}")
        return matching_tables[0]['id']

    def get_table_name_from_id(self, table_id: str) -> str:
        tables = self._get_tables()
        return [table['name'] for table in tables if table['id'] == table_id][0]

    def get_column_names_from_table(self, table_name: str) -> List[str]:
        columns = self._get_columns_from_table(table_name)
        return [column['name'] for column in columns]

    def add_table(self, table_name: str, table_config: DBObjectConfig):
        columns = []
        values = {}
        for att in table_config.attributes:
            column = self._create_synapse_column(att.name, att.datatype)
            columns.append(column)
            values[att.name] = []

        schema = sc.Schema(name=table_name, columns=columns, parent=self.project_id)
        table = sc.Table(schema, values)
        table = self.syn.store(table)

    def build_table(self, table_name:str, table: pd.DataFrame):
        """Adds a table to the project based on the input table

        Args:
            table_name (str): The name fo the table
            table (pd.DataFrame): A dataframe of the table
        """
        project = self.syn.get(self.project_id)
        table = sc.table.build_table(table_name, project, table)
        self.syn.store(table)

    def drop_table(self, table_name: str):
        synapse_id = self.get_table_id_from_name(table_name)
        self.syn.delete(synapse_id)

    def add_table_column(self, table_name: str, column_name: str, datatype: DBDatatype):
        column = self._create_synapse_column(column_name, datatype)
        column = self.syn.store(column)
        synapse_id = self.get_table_id_from_name(table_name)
        table = self.syn.get(synapse_id)
        table.addColumn(column)
        table = self.syn.store(table)

    def drop_table_column(self, table_name: str, column_name: str):
        synapse_id = self.get_table_id_from_name(table_name)
        table = self.syn.get(synapse_id)
        columns = self._get_columns_from_table(table_name)
        for col in columns:
            if col.name == column_name:
                table.removeColumn(col)
        table = self.syn.store(table)

    def execute_sql_statement(self, statement: str, include_row_data: bool = False):
        return self.syn.tableQuery(statement, includeRowIdAndRowVersion = include_row_data)

    def execute_sql_query(self, query: str, include_row_data: bool = False) -> pd.DataFrame:
        result = self.execute_sql_statement(query, include_row_data)
        table = pd.read_csv(result.filepath)
        return table

    def query_table(self, table_name: str, table_config: DBObjectConfig) -> pd.DataFrame:
        table_id = self.get_table_id_from_name(table_name)
        query = f"SELECT * FROM {table_id}"
        table = self.execute_sql_query(query)
        for att in table_config.attributes:
            if att.datatype == DBDatatype.Int:
                table = table.astype({att.name: 'Int64'})
            elif att.datatype == DBDatatype.Date:
                table[att.name] = pd.to_datetime(table[att.name], unit='ms').dt.date
            elif att.datatype == DBDatatype.Boolean:
                table = table.astype({att.name: 'boolean'})
        return table

    def insert_table_rows(self, table_name: str, data: pd.DataFrame):
        """Insert table rows

        Args:
            table_name (str): The name of the table to add rows into
            data (pd.DataFrame): The rows to be added.
        """
        synapse_id = self.get_table_id_from_name(table_name)
        table = self.syn.get(synapse_id)
        self.syn.store(sc.Table(table, data))

    # Need to figure out the best way of doing deletes and updates.
    def update_table_rows(self, table_name: str, data: pd.DataFrame):
        """ Placeholder
        """

    def delete_table_rows(self, table_name: str, data: pd.DataFrame, table_config: DBObjectConfig):
        pass

    def upsert_table_rows(self, table_name: str, data: pd.DataFrame):
        pass

    def _get_tables(self) -> List:
        project = self.syn.get(self.project_id)
        return list(self.syn.getChildren(project, includeTypes=['table']))

    def _get_columns_from_table(self, table_name: str) -> List[sc.Column]:
        synapse_id = self.get_table_id_from_name(table_name)
        return list(self.syn.getColumns(x=synapse_id))

    def _create_synapse_column(self, name: str, datatype: str) -> sc.Column:
        if datatype == DBDatatype.Text:
            syn_column = sc.Column(name=name, columnType='STRING', maximumSize=100)
        elif datatype == DBDatatype.Date:
            syn_column = sc.Column(name=name, columnType='DATE')
        elif datatype == DBDatatype.Int:
            syn_column =  sc.Column(name=name, columnType='INTEGER')
        elif datatype == DBDatatype.Float:
            syn_column = sc.Column(name=name, columnType='DOUBLE')
        elif datatype == DBDatatype.Boolean:
            syn_column= sc.Column(name=name, columnType='BOOLEAN')
        else:
            raise ValueError ()
        return syn_column
