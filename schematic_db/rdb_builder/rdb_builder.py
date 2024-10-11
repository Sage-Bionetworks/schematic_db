"""RDBBuilder"""

# pylint: disable=logging-fstring-interpolation
import logging
from schematic_db.databases.database_interface import Database
from schematic_db.schema_generator.schema_generator import (
    SchemaGenerator,
    DatabaseSchema,
)


logging.getLogger(__name__)


class RDBBuilder:  # pylint: disable=too-few-public-methods
    """Builds a database schema"""

    def __init__(self, rdb: Database, schema_generator: SchemaGenerator) -> None:
        """
        Args:
            rdb (Database): A relational database object
            schema (Schema): A Schema object
        """
        self.rdb = rdb
        self.schema_generator = schema_generator

    def build_database(self) -> None:
        """Builds the database based on the schema."""

        self._drop_all_tables()
        database_schema = self._get_database_schema()
        self._build_database_from_schema(database_schema)

    def _drop_all_tables(self) -> None:
        """Drops all tables from database and performs logging"""
        logging.info("Dropping all tables")
        self.rdb.drop_all_tables()
        logging.info("Dropped all tables")

    def _get_database_schema(self) -> DatabaseSchema:
        """Gets the database schema from the schema object, and performs logging

        Returns:
            DatabaseSchema: A generic schema for the database
        """
        logging.info("Getting database schema")
        database_schema = self.schema_generator.get_database_schema()
        logging.info("Got database schema")
        return database_schema

    def _build_database_from_schema(self, database_schema: DatabaseSchema) -> None:
        """Builds the database frm a generic schema, and performs logging

        Args:
            database_schema (DatabaseSchema): A generic schema for the database
        """
        logging.info("Building database")
        for table_schema in database_schema.table_schemas:
            logging.info(f"Adding table to database schema: {table_schema.name}")
            self.rdb.add_table(table_schema)
        logging.info("Database built")
