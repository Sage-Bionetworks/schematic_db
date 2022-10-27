"""
This package is used to manage backends in the FAIR data flow.

It is assumed that manifests are stored in a project at https://www.synapse.org/,
 and have been validated via schematic (https://github.com/Sage-Bionetworks/schematic),
 or the data curator app (https://github.com/Sage-Bionetworks/dcamodules).

Schematic_db makes use of the schematic API which is not currently public yet.
To setup the API instructions are at https://github.com/Sage-Bionetworks/schematic/tree/develop/api

The DB config classes are a generic representation of a database created from the json-ld file.

The Schema class is used to manage the Schematic API.

The Synapse class is used to mange all interactions with Synapse.

The various RDB classes are used to mange the chosen database backends.
Currently MySQL is supported.

The RDB Updater class uses a Schema object and RDB object to update or create a database using
 manifests stored in Synapse.

The Query store object is used to store queries run on the database.
Currently Synapse is supported.

The RDB_Queryer class is used to query a database and store the results somewhere.
"""
from .db_config import *
