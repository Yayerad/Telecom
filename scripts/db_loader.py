from sqlalchemy import create_engine
import pandas as pd
import mysql.connector
import pymysql

def load_data():
    try:
        # Create SQLAlchemy engine
        engine = create_engine('postgresql://postgres:root@localhost/telecom')

        # SQL Query to retrieve all data from the telecom table
        query = "SELECT * FROM xdr_data;"

        # Load data directly into a pandas DataFrame using the engine
        data = pd.read_sql(query, engine)

        # Close the engine connection
        engine.dispose()
        
        return data
    
    except Exception as e:
        print(f"Error loading data: {e}")
        return None
    
    
    


def export_to_mysql(df, db_user, db_password, db_host, db_name, table_name):
    """
    Exports a DataFrame to a MySQL database.

    :param df: DataFrame containing the data to export
    :param db_user: MySQL database username
    :param db_password: MySQL database password
    :param db_host: MySQL database host (e.g., 'localhost')
    :param db_name: Name of the MySQL database
    :param table_name: Name of the table where data will be inserted
    """
    try:
        # Create SQLAlchemy engine for MySQL connection
        engine = create_engine(f"mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_name}")

        # Export the dataframe to MySQL
        df[['Bearer Id', 'engagement_score', 'experience_score', 'satisfaction_score']].to_sql(
            table_name,
            con=engine,
            if_exists='replace',  # Options: 'fail', 'replace', 'append'
            index=False
        )

        print(f"Data successfully exported to {table_name} in the {db_name} database.")

    except Exception as e:
        print(f"Error occurred: {e}")


