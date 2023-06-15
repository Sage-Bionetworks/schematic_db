# Schematic DB

This package is used to manage backends in the FAIR data flow.

It is assumed that manifests are stored in a project at [Synapse](https://www.synapse.org/), and have been validated via [Schematic](https://github.com/Sage-Bionetworks/schematic), or the [Data Curator App](dca.app.sagebionetworks.org).

## Use as a package

### Installation

Schematic DB's is published to [PyPI](https://pypi.org/project/schematic-db/) whenever there is a new release.

Note that Schematic DB has 3 extra packages [mysql, postgres, synapse]. These serve as the database backend for Schematic DB. You will need one of these to run this package and can be installed like:

```bash
pip install schematic_db[synapse]
```

If using Schematic DB as part of a package you will want setup the `pyproject.toml` like `schematic-db = {version = "x.y.z", extras = ["synapse"]}`

### Usage

#### Schema object

The Schema class interacts with the [Schematic API](https://schematic.api.sagebionetworks.org/v1/ui/). It is used to create a database schema from a schema in json ld form.

It is assumed that you have setup a Synapse project where your manifests exist.

To create the schema:

```python
from schematic_db.schema import Schema, SchemaConfig
config = SchemaConfig(
        schema_url = "https://raw.githubusercontent.com/Sage-Bionetworks/Schematic-DB-Test-Schemas/main/test_schema.jsonld"
    )
schema = Schema(config)
```

#### Manifest Store object

The ManifestStore class is used to interact with the asset store in Synapse to download manifests needed to update the database.

It is assumed that you have setup a Synapse project where your manifests exist, and the appropriate fileview.

There are two types of ManifestStore classes.
The [APIManifestStore](https://github.com/Sage-Bionetworks/schematic_db/blob/main/schematic_db/manifest_store/api_manifest_store.py) class uses the Schematic API.
The [SynapseManifestStore](https://github.com/Sage-Bionetworks/schematic_db/blob/main/schematic_db/manifest_store/synapse_store.py) uses the  python Synapse client.

To create the manifest store:

```python
from schematic_db.manifest_store import APIManifestStore, SynapseManifestStore  ManifestStoreConfig
config = ManifestStoreConfig(
        schema_url = "https://raw.githubusercontent.com/Sage-Bionetworks/Schematic-DB-Test-Schemas/main/test_schema.jsonld",
        synapse_project_id = "syn1",
        synapse_asset_view_id = "syn2",
        synapse_input_token = "xxx"
    )
manifest_store = APIManifestStore(config)
# or manifest_store = SynapseManifestStore(config)
```

#### Relational database objects

The various database objects are how Schematic DB interacts with the underlying database

For a SQL based database:

```python
from schematic_db.rdb.sql_alchemy_database import SQLConfig
from schematic_db.rdb.mysql import MySQLDatabase
from schematic_db.rdb.postgres import PostgresDatabase

config = SQLConfig(
        username="username",
        password="pass",
        host="host",
        name="test_schema",
    )

database = PostgresDatabase(config)
database = MySQLDatabase(config)

```

For a Synapse based database:

```python
from schematic_db.rdb.synapse_database import SynapseDatabase

database =  SynapseDatabase(
    project_id="syn1,
    auth_token="xxx,
)
```

#### Building the database

The [RDBBuilder](https://github.com/Sage-Bionetworks/schematic_db/blob/main/schematic_db/rdb_builder/rdb_builder.py) class is responsible for building the database schema. Assuming you've built a Schema object, and RelationalDatabase object as described above, to build the database:

```python
from schematic_db.rdb_builder.rdb_builder import RDBBuilder

rdb_builder = RDBBuilder(rdb=database, schema=schema)
rdb_builder.build_database()
```

#### Updating the database

The [RDBUpdater](https://github.com/Sage-Bionetworks/schematic_db/blob/main/schematic_db/rdb_updater/rdb_updater.py) class is responsible for updating the database when there are new or updated manifests. It does NOT update the schema. Assuming you've built a ManifestStore object, and RelationalDatabase object as described above, to update the database:

```python
from schematic_db.rdb_updater.rdb_updater import RDBUpdater

RDBUpdater(rdb=database, manifest_store=manifest_store)
rdb_updater.update_database()
```

## Local Development

### Setup

Schematic DB can make use of various db(and non db) backends. Currently these include:

- [MySQL](https://dev.mysql.com/doc/mysql-installation-excerpt/5.7/en/)
- [Postgres](https://www.postgresql.org/download/)
- [Synapse](https://www.synapse.org/)

If you intend to use either MySQL or Postgres you will need to install those, and can follow the above links.

Clone the `schematic_db` package repository.

```bash
git clone https://github.com/Sage-Bionetworks/schematic_db.git
```

Install `poetry` (version 1.2 or later) using either the [official installer](https://python-poetry.org/docs/#installing-with-the-official-installer) or [pipx](https://python-poetry.org/docs/#installing-with-pipx). If you have an older installation of Poetry, we recommend uninstalling it first.

Start the virtual environment by doing:

```bash
poetry shell
```

To install all the packages you will have needed to install both MySQL and Postgres(See above)

Install all the dependencies by doing:

```bash
poetry install --all-extras
```

If you just want to install dependencies for one type of database (ie. Synapse):

```bash
poetry install -E synapse
```

### Tests

#### Secrets

Create a secrets file from template:

```bash
cp tests/data/example_secrets.yml tests/data/secrets.yml
```

`tests/data/secrets.yml` is in the `.gitignore` file so it can not be commited by git. There are sections for each type of database. Update the section(s) that are relevant to the code you will be working on.

If you want to work on and test code relating to Synapse Databases you will need a project in Synapse to test on. You will need to create an empty Synapse project. The `project_id` field is the Synapse id for the project. The `username` and `auth_token` must be for an account with edit and delete permission for the project.

#### Formatting tests

Before making a pull request you will want to make sure sure your code is formatted the correct way. Github will run the following tests:

```bash
mypy --disallow-untyped-defs .
black .
pylint schematic_db/* tests/*
```

You will want to make sure any changes passes the above tests before making a pull request.

#### Unit tests

Before making a pull request you will want to make sure sure your changes haven't broken any existing tests. The github workflow will do:

```bash
pytest
```

### Architecture

#### Documentation

Schematic db uses pdoc to generate documentation from docstrings. This exists [here](https://sage-bionetworks.github.io/schematic_db/schematic_db.html)

#### Classes

##### Database Schema

The [DatabaseSchema](https://github.com/Sage-Bionetworks/schematic_db/blob/main/schematic_db/db_schema/db_schema.py) class is used to store data for the database in form agnostic to the type. It is cerated by a [Schema](https://github.com/Sage-Bionetworks/schematic_db/blob/main/schematic_db/schema/schema.py) object, and used by the various relational database classes.

##### Schema

The [Schema](https://github.com/Sage-Bionetworks/schematic_db/blob/main/schematic_db/schema/schema.py) class is used to interact with the Schematic API to create a generic representation of the database from the schema. This is stored as a [DatabaseSchema](https://github.com/Sage-Bionetworks/schematic_db/blob/main/schematic_db/db_schema/db_schema.py) object. The Schema is used by the [RDBBuilder](https://github.com/Sage-Bionetworks/schematic_db/blob/main/schematic_db/rdb_builder/rdb_builder.py) class.

##### Manifest Store

The [ManifestStore](https://github.com/Sage-Bionetworks/schematic_db/blob/main/schematic_db/manifest_store/manifest_store.py) is an Abstract Base Class. This means it is meant to be inherited from and not used. It provides no functionality, just as interface. Inheriting from it implies that child class must implement it's methods with the same signature. The child classes(see blow) interact with the asset store in Synapse to download manifests needed to update the database. The ManifestStore is used by the [RDBUpdater](https://github.com/Sage-Bionetworks/schematic_db/blob/main/schematic_db/rdb_updater/rdb_updater.py) class.

The [APIManifestStore](https://github.com/Sage-Bionetworks/schematic_db/blob/main/schematic_db/manifest_store/api_manifest_store.py) class uses the Schematic API.
The [SynapseManifestStore](https://github.com/Sage-Bionetworks/schematic_db/blob/main/schematic_db/manifest_store/synapse_store.py) uses the python Synapse client.

##### Schema Graph

The [SchemaGraph](https://github.com/Sage-Bionetworks/schematic_db/blob/main/schematic_db/schema_graph/schema_graph.py)  class is used to interact with the Schematic API to create the schema in graph form, and is used by several other classes.

##### Synapse

The [Synapse](https://github.com/Sage-Bionetworks/schematic_db/tree/main/schematic_db/synapse) class if a facade for the SynapseClient library. It is used by several classes.

##### Relational Database

The [RelationalDatabase](https://github.com/Sage-Bionetworks/schematic_db/blob/main/schematic_db/rdb/rdb.py) is an Abstract Base Class. This means it is meant to be inherited from and not used. It provides no functionality, just as interface. Inheriting from it implies that child class must implement it's methods with the same signature. This is done so that all the database classes work the same.

The [SQLAlchemyDatabase](https://github.com/Sage-Bionetworks/schematic_db/blob/main/schematic_db/rdb/sql_alchemy_database.py) is not meant to be used, only be inherited from. It provides generic  SQLALchemy functionality.

The [MySQL](https://github.com/Sage-Bionetworks/schematic_db/blob/main/schematic_db/rdb/mysql.py) and [Postgres](https://github.com/Sage-Bionetworks/schematic_db/blob/main/schematic_db/rdb/postgres.py) classes both inherit from the SQLAlchemyDatabase and so inherit it's code and the RelationDatabase interface.

The [SynapseDatabase](https://github.com/Sage-Bionetworks/schematic_db/blob/main/schematic_db/rdb/synapse_database.py) inherits from the RelationDatabase, so it implements the RelationDatabase interface. It uses the [Synapse](https://github.com/Sage-Bionetworks/schematic_db/tree/main/schematic_db/synapse) class.

##### Query Store

The [QueryStore](https://github.com/Sage-Bionetworks/schematic_db/blob/main/schematic_db/query_store/query_store.py) is an Abstract Base Class. This means it is meant to be inherited from and not used. It provides no functionality, just as interface. Inheriting from it implies that child class must implement it's methods with the same signature. This is done so that all the query store classes work the same. The query store is used by the [RDBQueryer](https://github.com/Sage-Bionetworks/schematic_db/blob/main/schematic_db/rdb_queryer/rdb_queryer.py) class.

The [SynapseQueryStore](https://github.com/Sage-Bionetworks/schematic_db/blob/main/schematic_db/rdb/synapse_database.py) inherits from the QueryStore, so it implements its interface. It uses the [Synapse](https://github.com/Sage-Bionetworks/schematic_db/tree/main/schematic_db/synapse) class.

##### RDB Builder

The [RDBBuilder](https://github.com/Sage-Bionetworks/schematic_db/blob/main/schematic_db/rdb_builder/rdb_builder.py) class is responsible for building the database schema. It uses a [Schema](https://github.com/Sage-Bionetworks/schematic_db/blob/main/schematic_db/schema/schema.py) object to create a [DatabaseSchema](https://github.com/Sage-Bionetworks/schematic_db/blob/main/schematic_db/db_config/db_config.py). This is used to build each table using a [RelationalDatabase](https://github.com/Sage-Bionetworks/schematic_db/blob/main/schematic_db/rdb/rdb.py) object.

##### RDB Updater

The [RDBUpdater](https://github.com/Sage-Bionetworks/schematic_db/blob/main/schematic_db/rdb_updater/rdb_updater.py) class is responsible for updating the database when there are new or updated manifests. It does NOT update the schema. It uses a [ManifestStore](https://github.com/Sage-Bionetworks/schematic_db/blob/main/schematic_db/manifest_store/manifest_store.py) to download the manifests and a [RelationalDatabase](https://github.com/Sage-Bionetworks/schematic_db/blob/main/schematic_db/rdb/rdb.py) object to update the database.

##### RDB Queryer

The [RDBQueryer](https://github.com/Sage-Bionetworks/schematic_db/blob/main/schematic_db/rdb_queryer/rdb_queryer.py) class is used to query the database and store the results. It uses a [RelationalDatabase](https://github.com/Sage-Bionetworks/schematic_db/blob/main/schematic_db/rdb/rdb.py) object to query the database, and a [QueryStore](https://github.com/Sage-Bionetworks/schematic_db/blob/main/schematic_db/query_store/query_store.py) object to store the result.
