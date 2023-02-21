# Initialize Database

## Resume

In `init_database` folder `init.py` python file will:
- Create `stage` and `mart` schemas and all tables in `main` database
- Send request to Yandex API for generating data
- Then recive and insert this data to database in first raw layer - `stage` schema
- And after thats update tables second layer - `mart` schema

## Packages

- `init.py` - Main file thats call all dependend funcitons
- `funcs.py` - All functions thet actual do the main work
- `utils.py` - Utility
- `conns.py` - Connection to database
- `exceptions.py` - Initial step exceptions 

- `sql/*.sql` - SQL scripts thats called in `funcs.py` file. 

# DAGs

This DAG will request Yandex API for new batch of data of current data. If data 