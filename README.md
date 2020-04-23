# Songplays ETL (Data Warehouse)

Sparkify, a music streaming startup, has grown their user base and song database and want to move their processes and data onto the cloud. 

Here, we are building an ETL pipeline that extracts Sparkify data from S3, stages them in Redshift, and transforms data into a set of dimensional tables.

Songplays ETL is a Redshift ETL on songplays data created from files with songs data files and logs data files.

create_tables.py - Creates database and drops any existing tables prior to creating
etl.py - processes the files and inserts data into Redshift
sql_querys.py - has all the queries used in the project
dwh.cfg - A configuration file with AWS Redshift cluster connection details, IAM details and S3 bucket details where the files reside

## Installation of database connection wrapper

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install psycopg2.

```bash
pip install psycopg2
```

## Usage

```bash
python create_tables.py
python etl.py
```

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License
[MIT]
