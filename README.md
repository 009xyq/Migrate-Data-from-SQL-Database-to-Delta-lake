# Migrate-Data-from-SQL-Database-to-Delta-lake
## Project Original Thoughts:
As cloud platforms continue to gain widespread traction, an increasing number of companies are choosing to migrate their data to these platforms.

## Project Overview:
The goal of this project is to establish a data migration process using Databricks Delta Lake feature, which will process the data from SQL Database to Databricks Delta Lake.

## Project Technical Overview:
Simulate Change Data Capture (CDC) tool like Fivetran with Delta Lake Change Data Feed (CDF) feature to make the architecture simpler to implement and the MERGE operation and log versioning of Delta Lake.
Improve Delta performance by processing only changes following initial MERGE comparison to accelerate and simplify ETL/ELT operations.

### Pre requirement
1.	Library
```bash
pip3 install pyodbc
```
2.	Install and configure the Databricks CLI
3.	Use the Databricks CLI to create a secret scope
```bash
databricks secrets create-scope --scope <scope_name> --initial-manage-principal users
```
4.	Store secrets in the secret scope
```bash
databricks secrets put --scope <scope_name> --key <key_name> --string-value <secret_value>
```
5.	Access secrets: 
```bash
jdbcHostname = dbutils.secrets.get(scope="<scope_name>", key="<key_name>")
```
6.	install Microsoft ODBC driver 17 for SQL Server on Databricks
```bash in databricks
%sh
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
curl https://packages.microsoft.com/config/ubuntu/16.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
sudo apt-get update
sudo ACCEPT_EULA=Y apt-get -q -y install msodbcsql17
```
7.	configure connection_string
```python
connection_string = (f"DRIVER={odbcDriver};SERVER={odbcServer};DATABASE={jdbcDatabase};UID={jdbcUsername};PWD={jdbcPassword};""TrustServerCertificate=Yes")
```

## Project Approach:
1.	Create a database in Delta Lake, Ingest the table into Delta Lake using spark.read.jdbc, which is a Slowly Changing Dimensions (SCD) Type 1 table.
2.	Create another Delta table, which is a SCD Type 2 table.
3.	Make some changes in SQL Database.
4.	use the snapshot to sync it to the SCD Type 1 table in Delta Lake.
5.	Simulate CDC tool to sync changes made in the SQL Database to the SCD Type1 table in Delta Lake.
6.	Update SCD Type 2 table using CDF accordingly every time SCD Type 1 table were changed 
