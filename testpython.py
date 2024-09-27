#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pyodbc
import logging

# Configure logging for tests
logging.basicConfig(filename='test_log.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

try:
    # Define SQL Server connection details
    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};'
                          'SERVER=ims\\SQLEXPRESS;'
                          'DATABASE=WeatherDB;'
                          'Trusted_Connection=yes;')
    cursor = conn.cursor()

    # Check the number of records in the WeatherData table
    cursor.execute("SELECT COUNT(*) FROM WeatherData")
    row_count = cursor.fetchone()[0]
    logging.info(f"Number of rows in WeatherData: {row_count}")

    # Check for null DateTime values
    cursor.execute("SELECT COUNT(*) FROM WeatherData WHERE DateTime IS NULL")
    null_datetime_count = cursor.fetchone()[0]
    logging.info(f"Count of null DateTime: {null_datetime_count}")

    # Check average temperature
    cursor.execute("SELECT AVG(Temp_C) FROM WeatherData")
    average_temperature = cursor.fetchone()[0]
    logging.info(f"Average Temperature: {average_temperature:.2f}")

    # Additional checks can be added as needed

except Exception as e:
    logging.error(f"An error occurred during testing: {e}")

finally:
    # Close the connection
    if 'conn' in locals():
        conn.close()


# In[ ]:




