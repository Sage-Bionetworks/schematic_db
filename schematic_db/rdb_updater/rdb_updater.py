"""RDBUpdater"""
import warnings
import typing
import pandas as pd
from schematic_db.db_config import DBObjectConfig
from schematic_db.rdb import RelationalDatabase, UpdateDBTableError
from schematic_db.schema import Schema


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

    def update_all_database_tables(self, strict: typing.Optional[bool] = True) -> None:
        """Updates all tables in the db_config

        Args:
            strict (bool): If false, some errors are turned into warnings.
        """
        db_config = self.schema.create_db_config()
        for config in db_config.configs:
            self.update_database_table(config, strict)

    def update_database_table(
        self, table_config: DBObjectConfig, strict: typing.Optional[bool] = True
    ) -> None:
        """
        Updates a table in the database based on one or more manifests.
        If any of the manifests don't exist an exception will be raised.
        If the table doesn't exist in the database it will be built with the table config.

        Args:
            table_config (DBObjectConfig): A generic representation of the table as a
                DBObjectConfig object.
            strict (bool): If false, some errors are turned into warnings.

        Raises:
            NoManifestError: If strict = True, and Schema.get_manifests returns no manifests
        """
        manifest_tables = self.schema.get_manifests(table_config)
        # If there are no manifests and strict is true, raise an error.
        # Otherwise a warning and break out of function.
        if len(manifest_tables) == 0:
            msg = f"There were no manifests found for table: {table_config.name}"
            if strict:
                raise NoManifestError(msg)
            warnings.warn(NoManifestWarning(msg))
            return
        manifest_table = pd.concat(manifest_tables)
        manifest_table = manifest_table.drop_duplicates(
            subset=table_config.primary_keys
        )
        manifest_table.reset_index(inplace=True, drop=True)

        # If not strict try to update the table and return a warning instead of an error
        if strict:
            self.rdb.update_table(manifest_table, table_config)
        else:
            try:
                self.rdb.update_table(manifest_table, table_config)
            except UpdateDBTableError as error:
                warnings.warn(UpdateTableWarning(error.message))
