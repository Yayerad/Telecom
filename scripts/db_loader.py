# db_loader.py
import psycopg2
import pandas as pd

def load_data():
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(
            host="localhost",
            database="telecom",
            user="postgres",
            password="root"
        )
        cursor = conn.cursor()

        # SQL Query to retrieve all data from the telecom table
        query = """
            SELECT * FROM telecom_data;
        """
        
        # Load data into a pandas DataFrame
        data = pd.read_sql(query, conn)

        # Close the cursor and connection
        cursor.close()
        conn.close()
        
        return data
    
    except Exception as e:
        print(f"Error loading data: {e}")
        return None
