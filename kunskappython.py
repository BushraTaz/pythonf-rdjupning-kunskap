#!/usr/bin/env python
# coding: utf-8

# In[11]:


import pandas as pd
import pyodbc
import logging
import os

# Configure logging
logging.basicConfig(filename='process.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Define SQL Server connection parameters
server = 'ims\\SQLEXPRESS'
database = 'WeatherDB'
file_path = r'C:\Users\Imthias\Downloads\archive\Weather Data.csv'  # Ensure this path is correct

# Create a connection string with Windows Authentication
conn_str = (
    r'DRIVER={ODBC Driver 17 for SQL Server};'
    f'SERVER={server};'
    f'DATABASE={database};'
    'Trusted_Connection=yes;'
)

def main():
    try:
        # Check if the file exists
        if not os.path.isfile(file_path):
            raise FileNotFoundError(f"The file at {file_path} does not exist.")

        # Load the CSV file into a DataFrame
        df = pd.read_csv(file_path, sep='\t', parse_dates=['DateTime'], 
                         date_parser=lambda x: pd.to_datetime(x, format='%Y-%m-%d %H:%M:%S.%f'))

        # Print DataFrame columns to verify names
        logging.info("Original DataFrame columns: %s", df.columns.tolist())

        # Rename columns to match SQL table schema
        df.columns = [
            'DateTime',
            'Temp_C',
            'DewPointTemp_C',
            'RelHum_Percent',
            'WindSpeed_km_h',
            'Visibility_km',
            'Press_kPa',
            'Weather'
        ]

        # Verify column names after renaming
        logging.info("Updated DataFrame columns: %s", df.columns.tolist())

        # Data Cleaning (Optional)
        # Remove rows with NaN values in DateTime or Weather
        df.dropna(subset=['DateTime', 'Weather'], inplace=True)

        # Convert columns to appropriate data types
        df['Temp_C'] = pd.to_numeric(df['Temp_C'], errors='coerce')
        df['DewPointTemp_C'] = pd.to_numeric(df['DewPointTemp_C'], errors='coerce')
        df['RelHum_Percent'] = pd.to_numeric(df['RelHum_Percent'], errors='coerce')
        df['WindSpeed_km_h'] = pd.to_numeric(df['WindSpeed_km_h'], errors='coerce')
        df['Visibility_km'] = pd.to_numeric(df['Visibility_km'], errors='coerce')
        df['Press_kPa'] = pd.to_numeric(df['Press_kPa'], errors='coerce')

        # Connect to the database
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()

            # Check if the table exists and create it if not
            cursor.execute('''
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='WeatherData' AND xtype='U')
            BEGIN
                CREATE TABLE WeatherData (
                    [DateTime] DATETIME,
                    [Temp_C] FLOAT,
                    [DewPointTemp_C] FLOAT,
                    [RelHum_Percent] INT,
                    [WindSpeed_km_h] FLOAT,
                    [Visibility_km] FLOAT,
                    [Press_kPa] FLOAT,
                    [Weather] NVARCHAR(255)
                );
            END
            ''')
            conn.commit()

            # Insert data into SQL Server
            for index, row in df.iterrows():
                cursor.execute('''
                INSERT INTO WeatherData (DateTime, Temp_C, DewPointTemp_C, RelHum_Percent, WindSpeed_km_h, Visibility_km, Press_kPa, Weather)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', row['DateTime'], row['Temp_C'], row['DewPointTemp_C'], 
                               row['RelHum_Percent'], row['WindSpeed_km_h'], 
                               row['Visibility_km'], row['Press_kPa'], row['Weather'])

            conn.commit()
            logging.info('Data inserted successfully')

    except FileNotFoundError as fnf_error:
        logging.error(fnf_error)
    except pyodbc.Error as db_error:
        logging.error(f"Database error: {db_error}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()


# In[12]:


import pandas as pd
import pyodbc
import logging
import os

# Configure logging
logging.basicConfig(filename='process.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Define SQL Server connection parameters
server = 'ims\\SQLEXPRESS'
database = 'WeatherDB'
file_path = r'C:\Users\Imthias\Downloads\archive\Weather Data.csv'  # Ensure this path is correct

# Create a connection string with Windows Authentication
conn_str = (
    r'DRIVER={ODBC Driver 17 for SQL Server};'
    f'SERVER={server};'
    f'DATABASE={database};'
    'Trusted_Connection=yes;'
)

def main():
    try:
        # Check if the file exists
        if not os.path.isfile(file_path):
            raise FileNotFoundError(f"The file at {file_path} does not exist.")

        # Load the CSV file into a DataFrame
        df = pd.read_csv(file_path, sep=',', parse_dates=False)  # Change sep if needed

        # Print DataFrame columns to verify names
        logging.info("Original DataFrame columns: %s", df.columns.tolist())

        # If there's any discrepancy in column names, check and adjust here
        df.columns = df.columns.str.strip()  # Remove leading/trailing spaces

        # Print the updated DataFrame columns
        logging.info("Updated DataFrame columns: %s", df.columns.tolist())

        # Check if 'DateTime' is among the columns
        if 'DateTime' not in df.columns:
            raise KeyError("'DateTime' column not found in the CSV file.")

        # Rename columns to match SQL table schema
        df.columns = [
            'DateTime',
            'Temp_C',
            'DewPointTemp_C',
            'RelHum_Percent',
            'WindSpeed_km_h',
            'Visibility_km',
            'Press_kPa',
            'Weather'
        ]

        # Verify column names after renaming
        logging.info("Renamed DataFrame columns: %s", df.columns.tolist())

        # Data Cleaning (Optional)
        df.dropna(subset=['DateTime', 'Weather'], inplace=True)

        # Convert columns to appropriate data types
        df['Temp_C'] = pd.to_numeric(df['Temp_C'], errors='coerce')
        df['DewPointTemp_C'] = pd.to_numeric(df['DewPointTemp_C'], errors='coerce')
        df['RelHum_Percent'] = pd.to_numeric(df['RelHum_Percent'], errors='coerce')
        df['WindSpeed_km_h'] = pd.to_numeric(df['WindSpeed_km_h'], errors='coerce')
        df['Visibility_km'] = pd.to_numeric(df['Visibility_km'], errors='coerce')
        df['Press_kPa'] = pd.to_numeric(df['Press_kPa'], errors='coerce')

        # Connect to the database
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()

            # Check if the table exists and create it if not
            cursor.execute('''
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='WeatherData' AND xtype='U')
            BEGIN
                CREATE TABLE WeatherData (
                    [DateTime] DATETIME,
                    [Temp_C] FLOAT,
                    [DewPointTemp_C] FLOAT,
                    [RelHum_Percent] INT,
                    [WindSpeed_km_h] FLOAT,
                    [Visibility_km] FLOAT,
                    [Press_kPa] FLOAT,
                    [Weather] NVARCHAR(255)
                );
            END
            ''')
            conn.commit()

            # Insert data into SQL Server
            for index, row in df.iterrows():
                cursor.execute('''
                INSERT INTO WeatherData (DateTime, Temp_C, DewPointTemp_C, RelHum_Percent, WindSpeed_km_h, Visibility_km, Press_kPa, Weather)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', row['DateTime'], row['Temp_C'], row['DewPointTemp_C'], 
                               row['RelHum_Percent'], row['WindSpeed_km_h'], 
                               row['Visibility_km'], row['Press_kPa'], row['Weather'])

            conn.commit()
            logging.info('Data inserted successfully')

    except FileNotFoundError as fnf_error:
        logging.error(fnf_error)
    except KeyError as ke:
        logging.error(f"Key error: {ke}")
    except pyodbc.Error as db_error:
        logging.error(f"Database error: {db_error}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()


# In[13]:


import pandas as pd
import pyodbc
import logging
import os

# Configure logging
logging.basicConfig(filename='process.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Define SQL Server connection parameters
server = 'ims\\SQLEXPRESS'
database = 'WeatherDB'
file_path = r'C:\Users\Imthias\Downloads\archive\Weather Data.csv'  # Ensure this path is correct

# Create a connection string with Windows Authentication
conn_str = (
    r'DRIVER={ODBC Driver 17 for SQL Server};'
    f'SERVER={server};'
    f'DATABASE={database};'
    'Trusted_Connection=yes;'
)

def main():
    try:
        # Check if the file exists
        if not os.path.isfile(file_path):
            raise FileNotFoundError(f"The file at {file_path} does not exist.")

        # Load the CSV file into a DataFrame
        df = pd.read_csv(file_path, sep=',', parse_dates=False)  # Change sep if needed

        # Print DataFrame columns to verify names
        logging.info("Original DataFrame columns: %s", df.columns.tolist())

        # Strip column names to remove any leading/trailing whitespace
        df.columns = df.columns.str.strip()

        # Print the updated DataFrame columns
        logging.info("Updated DataFrame columns: %s", df.columns.tolist())

        # Rename columns to match SQL table schema
        df.rename(columns={
            'Date/Time': 'DateTime',
            'Dew Point Temp_C': 'DewPointTemp_C',
            'Rel Hum_%': 'RelHum_Percent',
            'Wind Speed_km/h': 'WindSpeed_km_h'
        }, inplace=True)

        # Verify column names after renaming
        logging.info("Renamed DataFrame columns: %s", df.columns.tolist())

        # Check if 'DateTime' is among the columns
        if 'DateTime' not in df.columns:
            raise KeyError("'DateTime' column not found in the DataFrame.")

        # Data Cleaning (Optional)
        df.dropna(subset=['DateTime', 'Weather'], inplace=True)

        # Convert columns to appropriate data types
        df['Temp_C'] = pd.to_numeric(df['Temp_C'], errors='coerce')
        df['DewPointTemp_C'] = pd.to_numeric(df['DewPointTemp_C'], errors='coerce')
        df['RelHum_Percent'] = pd.to_numeric(df['RelHum_Percent'], errors='coerce')
        df['WindSpeed_km_h'] = pd.to_numeric(df['WindSpeed_km_h'], errors='coerce')
        df['Visibility_km'] = pd.to_numeric(df['Visibility_km'], errors='coerce')
        df['Press_kPa'] = pd.to_numeric(df['Press_kPa'], errors='coerce')

        # Connect to the database
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()

            # Check if the table exists and create it if not
            cursor.execute('''
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='WeatherData' AND xtype='U')
            BEGIN
                CREATE TABLE WeatherData (
                    [DateTime] DATETIME,
                    [Temp_C] FLOAT,
                    [DewPointTemp_C] FLOAT,
                    [RelHum_Percent] INT,
                    [WindSpeed_km_h] FLOAT,
                    [Visibility_km] FLOAT,
                    [Press_kPa] FLOAT,
                    [Weather] NVARCHAR(255)
                );
            END
            ''')
            conn.commit()

            # Insert data into SQL Server
            for index, row in df.iterrows():
                cursor.execute('''
                INSERT INTO WeatherData (DateTime, Temp_C, DewPointTemp_C, RelHum_Percent, WindSpeed_km_h, Visibility_km, Press_kPa, Weather)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', row['DateTime'], row['Temp_C'], row['DewPointTemp_C'], 
                               row['RelHum_Percent'], row['WindSpeed_km_h'], 
                               row['Visibility_km'], row['Press_kPa'], row['Weather'])

            conn.commit()
            logging.info('Data inserted successfully')

    except FileNotFoundError as fnf_error:
        logging.error(fnf_error)
    except KeyError as ke:
        logging.error(f"Key error: {ke}")
    except pyodbc.Error as db_error:
        logging.error(f"Database error: {db_error}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()


# In[14]:


import pandas as pd
import pyodbc
import logging

# Configure logging
logging.basicConfig(filename='data_validation.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Define SQL Server connection details
conn_str = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=ims\\SQLEXPRESS;DATABASE=WeatherDB;Trusted_Connection=yes;'

try:
    # Connect to the database
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    
    # Validate data
    # Count total rows in WeatherData
    cursor.execute("SELECT COUNT(*) FROM WeatherData;")
    row_count = cursor.fetchone()[0]

    # Count null DateTime entries
    cursor.execute("SELECT COUNT(*) FROM WeatherData WHERE DateTime IS NULL;")
    null_datetime_count = cursor.fetchone()[0]

    # Calculate average temperature
    cursor.execute("SELECT AVG(Temp_C) FROM WeatherData;")
    average_temperature = cursor.fetchone()[0]

    # Log the results
    logging.info("Number of rows in WeatherData: %d", row_count)
    logging.info("Count of null DateTime: %d", null_datetime_count)
    logging.info("Average Temperature: %.2f", average_temperature)

except Exception as e:
    logging.error(f"An error occurred: {e}")
finally:
    # Close the connection
    if 'conn' in locals():
        conn.close()


# In[ ]:




