from sqlalchemy import create_engine
import pandas as pd

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

