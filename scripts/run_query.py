from sqlalchemy import create_engine
import pandas as pd
def execute_query(query):
    try:
        # Create SQLAlchemy engine
        engine = create_engine('postgresql://postgres:root@localhost/telecom')

        # Execute the provided SQL query and load the result into a pandas DataFrame
        result_data = pd.read_sql(query, engine)

        # Close the engine connection
        engine.dispose()
        
        return result_data
    
    except Exception as e:
        print(f"Error executing query: {e}")
        return None