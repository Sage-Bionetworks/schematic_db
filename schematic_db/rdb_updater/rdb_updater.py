"""RDBUpdater"""

# pylint: disable=logging-fstring-interpolation
import warnings
import logging
from typing import Literal

import pandas as pd
from schematic_db.rdb.rdb import (
    RelationalDatabase,
    InsertDatabaseError,
)
from schematic_db.manifest_store.manifest_store import ManifestStore
from schematic_db.db_schema.db_schema import TableSchema
from schematic_db.utils.api_utils import ManifestMetadataList
from schematic_db.utils.dataframe_utils import split_table_into_chunks


logging.getLogger(__name__)

UpdateMethod = Literal["insert", "upsert"]


class NoManifestWarning(Warning):
    """Raised when trying to update a database table there are no manifests"""

    def __init__(
        self, table_name: str, manifest_metadata_list: ManifestMetadataList
    ) -> None:
        """_summary_

        Args:
            table_name (str): The name of the table there were no manifests for
            manifest_metadata_list (ManifestMetadataList): A list of metadata
             for all found manifests
        """
        self.message = "There were no manifests found for table"
        self.table_name = table_name
        self.manifest_metadata_list = manifest_metadata_list
        super().__init__(self.message)

    def __str__(self) -> str:
        return (
            f"{self.message}; "
            f"Table Name: {self.table_name}; "
            f"Manifests: {self.manifest_metadata_list}"
        )


class UpdateError(Exception):
    """Raised when there is an error doing a table update"""

    def __init__(self, table_name: str, dataset_id: str) -> None:
        """
        Args:
            table_name (str): The name of the table the upsert occurred in
            dataset_id (str): The dataset id of the manifest that was being used to update
        """
        self.message = "Error updating table"
        self.table_name = table_name
        self.dataset_id = dataset_id
        super().__init__(self.message)

    def __str__(self) -> str:
        return (
            f"{self.message}; "
            f"Table Name: {self.table_name}; "
            f"Dataset ID: {self.dataset_id}"
        )


class ManifestPrimaryKeyError(Exception):
    """Raised when a manifest is missing its primary key"""

    def __init__(
        self, table_name: str, dataset_id: str, primary_key: str, columns: list[str]
    ) -> None:
        """
        Args:
            table_name (str): The name of the table for which the manifest was downloaded
            dataset_id (str): The dataset id of the manifest
            primary_key (str): The primary key of the table
            columns (list[str]): The columns in the manifest
        """
        self.message = "Manifest is missing its primary key"
        self.table_name = table_name
        self.dataset_id = dataset_id
        self.primary_key = primary_key
        self.columns = columns
        super().__init__(self.message)

    def __str__(self) -> str:
        return (
            f"{self.message}; "
            f"Table Name: {self.table_name}; "
            f"Dataset ID: {self.dataset_id}; "
            f"Primary Key: {self.primary_key}; "
            f"Columns: [{','.join(self.columns)}]"
        )


class RDBUpdater:
    """An for updating a database."""

    def __init__(self, rdb: RelationalDatabase, manifest_store: ManifestStore) -> None:
        """
        Args:
            rdb (RelationalDatabase): A relational database object to be updated
            manifest_store (ManifestStore): A manifest store object to get manifests from
        """
        self.rdb = rdb
        self.manifest_store = manifest_store

    def update_database(
        self,
        method: UpdateMethod = "upsert",
        table_names: list[str] | None = None,
        chunk_size: int | None = None,
    ) -> None:
        """Updates all tables in database

        Args:
            method (UpdateMethod): The method used to update each table. Defaults to "upsert".
            table_names (list[str] | None): If not None, only these tables will be updated
            chunk_size (int | None): When updating a database table, this splits the the input
                table into chunks first. Each table will be split into chunks with row number equal
                to this parameter. This can be used when the update includes large tables.
        """
        logging.info("Updating database")
        tables_to_update = self.manifest_store.create_sorted_table_name_list()
        if table_names is not None:
            tables_to_update = [
                table for table in tables_to_update if table in table_names
            ]
        for name in tables_to_update:
            self.update_table(name, method=method, chunk_size=chunk_size)
        logging.info("Database updated")

    def update_table(
        self,
        table_name: str,
        method: UpdateMethod = "upsert",
        chunk_size: int | None = None,
    ) -> None:
        """
        Updates a table in the database based on one or more manifests.
        If any of the manifests don't exist a warning will be raised.

        Args:
            table_name (str): The name of the table to be updated
            method (UpdateMethod): The method used to update each table. Defaults to "upsert".
            chunk_size (int | None): When updating a database table, this splits the the input
                table into chunks first. The table will be split into chunks with row number equal
                to this parameter. This can be used when the update includes large tables.
        """
        manifest_ids = self.manifest_store.get_manifest_ids(table_name)

        # If there are no manifests a warning is raised and breaks out of function.
        if len(manifest_ids) == 0:
            warnings.warn(
                NoManifestWarning(
                    table_name, self.manifest_store.get_manifest_metadata()
                )
            )
            return

        for manifest_id in manifest_ids:
            self._update_table_with_manifest_id(
                table_name, manifest_id, method, chunk_size
            )

    def _update_table_with_manifest_id(
        self,
        table_name: str,
        manifest_id: str,
        method: UpdateMethod = "upsert",
        chunk_size: int | None = None,
    ) -> None:
        """Updates a table in the database with a manifest

        Args:
            table_name (str): The name of the table
            manifest_id (str): The id of the manifest
            method (UpdateMethod): The method used to update each table. Defaults to "upsert".
            chunk_size (int | None): When updating a database table, this splits the the input
                table into chunks first. The table will be split into chunks with row number equal
                to this parameter. This can be used when the update includes large tables.

        Raises:
            ManifestPrimaryKeyError: Raised when the manifest table is missing its primary key
            UpsertError: Raised when there is an UpsertDatabaseError caught
        """
        table_schema = self.rdb.get_table_schema(table_name)

        manifest_table = self._download_manifest(table_name, manifest_id)

        if table_schema.primary_key not in list(manifest_table.columns):
            raise ManifestPrimaryKeyError(
                table_name,
                manifest_id,
                table_schema.primary_key,
                list(manifest_table.columns),
            )

        normalized_table = self._normalize_table(manifest_table, table_schema)

        self._update_table_with_manifest(
            normalized_table, table_name, manifest_id, method, chunk_size
        )

    def _download_manifest(self, table_name: str, manifest_id: str) -> pd.DataFrame:
        """Downloads a manifest, and performs logging

        Args:
            table_name (str): The name of the table the manifest will be upserted into
            manifest_id (str): The id of the manifest

        Returns:
            (pd.DataFrame): The manifest in pandas.Dataframe form
        """
        logging.info(
            f"Downloading manifest; table name: {table_name}; manifest id: {manifest_id}"
        )
        manifest_table: pd.DataFrame = self.manifest_store.download_manifest(
            manifest_id
        )
        logging.info("Finished downloading manifest")
        return manifest_table

    def _normalize_table(
        self,
        table: pd.DataFrame,
        table_schema: TableSchema,
    ) -> pd.DataFrame:
        """
        Gets the table ready for upsert by selecting only needed columns and removing
         duplicate entries

        Args:
            table (pd.DataFrame): The table to normalize
            table_schema (TableSchema):The schema of the table

        Returns:
            pd.DataFrame: A normalized table
        """
        table_columns = set(table_schema.get_column_names())
        manifest_columns = set(table.columns)
        columns = list(table_columns.intersection(manifest_columns))
        table = table[columns]
        table = table.drop_duplicates(subset=table_schema.primary_key)
        table.reset_index(inplace=True, drop=True)
        return table

    def _update_table_with_manifest(  # pylint: disable=too-many-arguments
        self,
        table: pd.DataFrame,
        table_name: str,
        manifest_id: str,
        method: UpdateMethod = "upsert",
        chunk_size: int | None = None,
    ) -> None:
        """Updates the database table with the input table and performs logging

        Args:
            table (pd.DataFrame): The table to be upserted
            table_name (str): The name of the table to be upserted into
            manifest_id (str): The id of the manifest
            method (UpdateMethod): The method used to update each table. Defaults to "upsert".
            chunk_size (int | None): When updating a database table, this splits the the input
                table into chunks first. The table will be split into chunks with row number equal
                to this parameter. This can be used when the update includes large tables.

        Raises:
            UpdateError: Raised when there is an UpsertDatabaseError or InsertDatabaseError caught
            ValueError: Raised when method is not one of ['insert', 'upsert']
        """
        logging.info(
            f"Updating table with manifest; table name: {table_name}; manifest id: {manifest_id}"
        )
        split_tables = split_table_into_chunks(table, chunk_size)
        try:
            for i, split_table in enumerate(split_tables):
                logging.info(f"Updating table chunk no. #{i}")
                if method == "upsert":
                    self.rdb.upsert_table_rows(table_name, split_table)
                else:
                    self.rdb.insert_table_rows(table_name, split_table)
        except InsertDatabaseError as exc:
            raise UpdateError(table_name, manifest_id) from exc
        logging.info("Finished updating table")
