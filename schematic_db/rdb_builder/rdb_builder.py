"""RDBBuilder"""
from schematic_db.rdb.rdb import RelationalDatabase
from schematic_db.schema.schema import Schema


class RDBBuilder:  # pylint: disable=too-few-public-methods
    """Builds a database schema"""

    def __init__(self, rdb: RelationalDatabase, schema: Schema) -> None:
        self.rdb = rdb
        self.schema = schema

    def build_database(self) -> None:
        """
        Builds the database based on the schema.
        """
        self.rdb.drop_all_tables()
        db_config = self.schema.get_db_config()
        for config in db_config.configs:
            self.rdb.add_table(config.name, config)
