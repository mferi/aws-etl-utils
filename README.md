# AWS ETL Utils
Set of utilities to construct *data pipeline units in Amazon Web Services*.


## Redshift module: RedshiftETLBuilder
Methods to build ETL in Redshift.

### Features

RedshiftETLBuilder currently has these features:

- Connect to a given redshift cluster
- Passes parameters to sql scripts
- Execute sql scripts


## Test
Unit tests: python -m unittest discover

## Version
This utility uses bumpversion to update version on '.bumpversion.cfg', 'setup.py' and '.properties' files.
>> bumpversion --current-version 0.1.0 [major, minor, patch]
