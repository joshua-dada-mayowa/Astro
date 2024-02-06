# Orchestrated Data Migration: ELT with Docker Airflow Python MSSQL and Snowflake
Orchestrated Data Migration.' This cutting-edge endeavor leverages the power of Docker, Airflow, Python, MSSQL, and Snowflake to seamlessly execute Extract, Load, and Transform (ELT) processes

## Project Structure

```
Astro/
|-- .astro
|-- dags/
|   |-- dag.py
|   |-- full_load.py
|   |-- mssql_to_snowflake.py
|-- tests/
|-- .airflowignore
|-- .dockerignore
|-- .gitignore
|-- Dockerfile
|-- README.md
|-- Stop-Process
|-- packages.txt
|-- requirements.txt
```

### Directories and Files:

- **dags/**: Contains the Airflow DAGs for data migration.
  - **dag.py**: Main DAG definition.
  - **full_load.py**: DAG for full data load.
  - **mssql_to_snowflake.py**: DAG for migrating data from MSSQL to Snowflake.
- **tests/**: Contains test scripts for the project.
- **Dockerfile**: Configuration for building the Docker image.
- **packages.txt**: Lists packages to be installed in the Docker image.
- **requirements.txt**: Python dependencies for the project.

## Detailed Description of DAGs

### DAG 1: `full_load.py`

This DAG orchestrates the full load of data from MSSQL to Snowflake. It runs daily and includes the following tasks:

1. **Sensor Task (`wait_for_tables`)**: Waits until tables are available in the MSSQL database.
2. **Python Operator (`transfer_data`)**: Transfers data from MSSQL to Snowflake. This task dynamically fetches tables from MSSQL, extracts data, and loads it into Snowflake using temporary CSV files.

### DAG 2: `mssql_to_snowflake.py`

This DAG orchestrates the migration of specific tables from MSSQL to Snowflake. It includes the following tasks:

1. **Operators**: For each table specified in the `tables` list, the DAG dynamically creates tasks to:
   - Extract data from MSSQL using the `MsSqlOperator`.
   - Create tables in Snowflake using the `SnowflakeOperator`.
   - Load data into Snowflake using the `SnowflakeOperator`.

### DAG 3: `dag.py`
This DAG extracts data from the MSSQL database, creates corresponding tables in Snowflake, and loads the data into Snowflake. The data extraction, table creation, and data loading are executed in sequence for each table.


## Getting Started

To use the Astro Data Migration Project for migrating data from MSSQL to Snowflake, follow these steps:

1. **Setup Docker**: Ensure Docker is installed on your system.

2. **Install Astro CLI**: Install the Astro CLI for container orchestration.

3. **Setup MSSQL on Docker**: Install and configure MSSQL on Docker, and import your database from your local PC into the MSSQL container.

4. **Create Docker Network**: Create a Docker network to facilitate communication between the Astro CLI containers (trigger, webserver, scheduler, postgres) and the MSSQL container. Run the following command:

   ```bash
   docker network create astro_network
   ```

   This command will create a Docker network named `astro_network`.

5. **Clone the Repository**: Clone the Astro repository to your local machine.

6. **Initialize Astro Development Environment**: Optionally, initialize the Astro development environment by running `astro dev init`.

7. **Start Astro Development Environment**: Run `astro dev start --wait=5m` to start the Astro development environment. This command will start the Airflow environment using Astro CLI.

8. **Configure Airflow Connections**: On the Airflow UI, configure the `snowflake_default` and `mssql_default` connections as per your Snowflake and MSSQL configurations.


## Contributing

Contributions to the Astro Data Migration Project are welcome! If you have any suggestions, find issues, or want to contribute code, please open an [issue](https://github.com/joshua-dada-mayowa/Astro/issues) or submit a pull request.

