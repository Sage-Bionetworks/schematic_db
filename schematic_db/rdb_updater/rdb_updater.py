"""RDBUpdater"""
import warnings
import pandas as pd
from schematic_db.db_config import DBConfig, DBObjectConfig, DBDatatype
from schematic_db.rdb import RelationalDatabase
from schematic_db.schema import Schema

DATATYPES = {
    DBDatatype.TEXT: "string",
    DBDatatype.DATE: "datetime64[ns]",
    DBDatatype.INT: "int64",
    DBDatatype.FLOAT: "float64",
}


class SchemaConflictError(Exception):
    """Raised when the current database schema is different than the incoming schema"""

    def __init__(self) -> None:
        self.message = (
            "The schema generated from the schema object is different from the current database."
            "Please use build_database() instead."
        )
        super().__init__(self.message)

    def __str__(self) -> str:
        return self.message


class NoManifestWarning(Warning):
    """Raised when trying to update a database table there are no manifests"""

    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(self.message)


class NoManifestError(Exception):
    """Raised when trying to update a database table there are no manifests"""

    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(self.message)

    def __str__(self) -> str:
        return self.message


class UpdateTableWarning(Warning):
    """
    Occurs when trying to update a database table and the rdb subclass encounters an error
    """

    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(self.message)


class RDBUpdater:
    """An for updating a database."""

    def __init__(self, rdb: RelationalDatabase, schema: Schema) -> None:
        self.rdb = rdb
        self.schema = schema

    def build_database(self) -> None:
        """
        Builds the database based on the schema.
        If the database exists all tables will be dropped and the database recreated.
        """
        self.rdb.drop_all_tables()
        db_config = self.schema.get_db_config()
        self._update_database(db_config)

    def update_database(self) -> None:
        """
        Updates all tables in the schema.
        If the schema has changed since last update an exception will be thrown,
         the user should use self.build_database() instead
        """
        new_db_config = self.schema.get_db_config()
        current_db_config = self.rdb.get_db_config()
        if new_db_config != current_db_config:
            raise SchemaConflictError()
        self._update_database(new_db_config)

    def _update_database(self, db_config: DBConfig) -> None:
        """Updates all tables in the schema."""
        for config in db_config.configs:
            self.update_database_table(config)

    def update_database_table(self, table_config: DBObjectConfig) -> None:
        """
        Updates a table in the database based on one or more manifests.
        If any of the manifests don't exist an exception will be raised.
        If the table doesn't exist in the database it will be built with the table config.

        Args:
            table_config (DBObjectConfig): A generic representation of the table as a
                DBObjectConfig object.
        """
        manifest_tables = self.schema.get_manifests(table_config)
        # If there are no manifests a warning is raised and breaks out of function.
        if len(manifest_tables) == 0:
            msg = f"There were no manifests found for table: {table_config.name}"
            warnings.warn(NoManifestWarning(msg))
            return

        # combine and normalize manifests into one table
        manifest_table = pd.concat(manifest_tables)
        manifest_table = manifest_table.drop_duplicates(subset=table_config.primary_key)
        manifest_table.reset_index(inplace=True, drop=True)

        # cast columns as types defined in config
        for col in manifest_table.columns:
            col_attribute = table_config.get_attribute_by_name(col)
            pandas_col_type = DATATYPES[col_attribute.datatype]
            manifest_table[col] = manifest_table[col].astype(pandas_col_type)

        self.rdb.update_table(manifest_table, table_config)
