import psycopg2

def calculate_and_update_zscores(conn_string):
    
    # Retrieve database credentials from environment variables
    
    # Form the connection string
    try:
        # Step 1: Connect to the PostgreSQL database using the connection string
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()
        
        # Step 2: Compute mean and standard deviation for the last 3 days
        stats_query = """
        WITH stats AS (
            SELECT 
                AVG(measurement) AS mean,
                STDDEV(measurement) AS stddev
            FROM ztest
            WHERE datetime >= NOW() - INTERVAL '3 days'
        )
        -- Step 3: Compute Z-Scores
        SELECT 
            ztest.datetime,
            ztest.measurement,
            (ztest.measurement - stats.mean) / stats.stddev AS zscore
        FROM ztest, stats
        WHERE datetime >= NOW() - INTERVAL '3 days';
        """
        cursor.execute(stats_query)
        rows = cursor.fetchall()
        if not rows:
            print("No data available for the last 3 days to calculate z-scores.")
            return
        
        # Step 4: Update Z-Scores in the table
        update_query = """
        UPDATE ztest
        SET zscore = data.zscore
        FROM (
            SELECT 
                datetime,
                (measurement - stats.mean) / stats.stddev AS zscore
            FROM ztest, (
                SELECT 
                    AVG(measurement) AS mean,
                    STDDEV(measurement) AS stddev
                FROM ztest
                WHERE datetime >= NOW() - INTERVAL '3 days'
            ) stats
            WHERE datetime >= NOW() - INTERVAL '3 days'
        ) AS data
        WHERE ztest.datetime = data.datetime;
        """
        cursor.execute(update_query)
        conn.commit()
        print(f"Z-scores updated successfully for the last 3 days.")
    
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def main(): 
        # Run the zscore calculation and update function
        calculate_and_update_zscores.local()