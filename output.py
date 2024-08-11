
import psycopg2
import configparser

# Load connection details from the configuration file
config = configparser.ConfigParser()
config.read('connection.cfg')

# Extract connection parameters
database = config['database']['dbname']
user = config['database']['user']
password = config['database']['password']
host = config['database']['host']
port = config['database']['port']

# SQL commands to create the tables
create_tables_sql = """
BEGIN;
CREATE TABLE IF NOT EXISTS public."Cities"
(
    id integer NOT NULL,
    city_name "char"[] NOT NULL,
    population_density real,
    population integer,
    residential_buildings integer,
    sold_apartments_per_year integer,
    average_apartment_price_1m2 real,
    median_apartment_price_1m2 real,
    vacancies integer,
    average_salary real,
    CONSTRAINT "Cities_pkey" PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public."Apartment_prices"
(
    id "char"[] NOT NULL,
    city_id integer NOT NULL,
    square_meters integer,
    rooms integer,
    floors_number integer,
    floor integer,
    built_year integer,
    latitude real,
    longitude real,
    centre_distance real,
    poi_count integer,
    school_distance real,
    clinic_distance real,
    post_office_distance real,
    kindergarden_distance real,
    restaurant_distance real,
    college_distance real,
    pharmacy_distance real,
    ownership "char"[],
    building_material "char"[],
    condition "char"[],
    has_parking_space boolean,
    has_balcony boolean,
    has_elevator boolean,
    "has_security" boolean,
    has_storage_room boolean,
    price integer NOT NULL,
    CONSTRAINT "Apartment_prices_pkey" PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public."Sofware_jobs"
(
    id integer NOT NULL,
    city_id integer NOT NULL,
    company "char"[],
    company_size real,
    technology "char"[],
    seniority "char"[],
    emp_salary_min real,
    emp_salary_max real,
    b2b_salary_min real,
    b2b_salary_max real,
    CONSTRAINT "Sofware_jobs_pkey" PRIMARY KEY (id)
);

ALTER TABLE IF EXISTS public."Apartment_prices"
    ADD CONSTRAINT "City_ref" FOREIGN KEY (city_id)
    REFERENCES public."Cities" (id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;

ALTER TABLE IF EXISTS public."Sofware_jobs"
    ADD CONSTRAINT "City_ref" FOREIGN KEY (city_id)
    REFERENCES public."Cities" (id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

END;
"""

# Connect to PostgreSQL and create the tables
try:
    # Establish the database connection
    conn = psycopg2.connect(
        dbname=database,
        user=user,
        password=password,
        host=host,
        port=port
    )

    # Create a cursor object
    cursor = conn.cursor()

    # Execute the SQL command to create tables
    cursor.execute(create_tables_sql)

    # Commit the transaction
    conn.commit()

    print("Tables created successfully.")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Close the database connection
    if cursor:
        cursor.close()
    if conn:
        conn.close()



import pandas as pd
import glob

# Load apartment prices from CSV files
apartment_files = glob.glob('./Data/apartments_pl_2023_0[8-9].csv') + glob.glob('./Data/apartments_pl_2024_0[1-6].csv')
apartments_prices = pd.concat([pd.read_csv(f) for f in apartment_files], ignore_index=True)

# Load job offers from CSV files
job_files = glob.glob('./Data/2023[0-9][^0-9]*_soft_eng_jobs_pol.csv') + glob.glob('./Data/2024[0-9][^0-9]*_soft_eng_jobs_pol.csv')
jobs_offers = pd.concat([pd.read_csv(f) for f in job_files], ignore_index=True)

# Load city data from semicolon-separated CSV files
residential_buildings = pd.read_csv('./Data/GOSP_2909_CTAB_20240731215208.csv', sep=';')
apartments_sold = pd.read_csv('./Data/RYNE_3783_CTAB_20240731215821.csv', sep=';')
median_prices = pd.read_csv('./Data/RYNE_3787_CTAB_20240731215911.csv', sep=';')
mean_prices = pd.read_csv('./Data/RYNE_3788_CTAB_20240731215943.csv', sep=';')
vacancies = pd.read_csv('./Data/RYNE_4294_CTAB_20240731220115.csv', sep=';')
average_salary = pd.read_csv('./Data/WYNA_2497_CTAB_20240731220629.csv', sep=';')



import pandas as pd
import numpy as np

# Define a function to clean city names
def clean_city_names(city_name):
    diacritical_map = {
        'ą': 'a', 'ć': 'c', 'ę': 'e', 'ł': 'l', 'ń': 'n', 'ó': 'o', 'ś': 's', 'ź': 'z', 'ż': 'z',
        'Ą': 'A', 'Ć': 'C', 'Ę': 'E', 'Ł': 'L', 'Ń': 'N', 'Ó': 'O', 'Ś': 'S', 'Ź': 'Z', 'Ż': 'Z'
    }
    for diacritic, replacement in diacritical_map.items():
        city_name = city_name.replace(diacritic, replacement)
    return city_name

# Load the dataframes
city_data = [residential_buildings, apartments_sold, median_prices, mean_prices, vacancies, average_salary]

# Remove columns from respective dataframes
for df in city_data:
    if df.name == "apartments_sold":
        df.drop(columns=['Kod'], inplace=True)
    if df.name == "median_price":
        df.drop(columns=['Kod'], inplace=True)
    if df.name == "mean_price":
        df.drop(columns=['Kod'], inplace=True)
    if df.name == "vacancies":
        df.drop(columns=['Kod'], inplace=True)
    if df.name == "avg_salary":
        df.drop(columns=['Kod'], inplace=True)

# Map "yes"/"no" to True/False in apartments_prices
boolean_columns = ['hasParkingSpace', 'hasBalcony', 'hasElevator', 'hasSecurity', 'hasStorageRoom']
for col in boolean_columns:
    apartments_prices[col] = apartments_prices[col].map({'yes': True, 'no': False})

# Capitalize city names and fix diacritics in apartments_prices
apartments_prices['city'] = apartments_prices['city'].apply(lambda x: clean_city_names(x).capitalize())

# Process specific dataframe rules
for df in city_data:
    if df.name in ["residential_buildings", "apartments_sold", "median_price", "mean_price", "avg_salary"]:
        df['Nazwa'] = df['Nazwa'].str.replace(r'Powiat m\. ', '', regex=True)
    if df.name == "vacancies":
        voivodeship_mapping = {
            # Add actual mappings of Polish voivodeships to capitals
            # e.g. 'Mazowieckie': 'Warsaw'
        }
        df['Nazwa'] = df['Nazwa'].replace(voivodeship_mapping)

# Join dataframes on "Nazwa"
merged_data = apartments_prices
for df in city_data:
    merged_data = merged_data.merge(df, on='Nazwa', how='left')

# Rename columns
apartments_prices.rename(columns={'city': 'city_id'}, inplace=True)
jobs_offers.rename(columns={'location': 'city_id'}, inplace=True)

# Map city_ids from city_data to apartments_prices and jobs_offers
city_id_map = city_data[['Kod', 'Nazwa']].set_index('Nazwa')['Kod'].to_dict()
apartments_prices['city_id'] = apartments_prices['city'].map(city_id_map)
jobs_offers['city_id'] = jobs_offers['city_id'].map(city_id_map)



import pandas as pd
import psycopg2
import configparser

# Load connection details from the configuration file
config = configparser.ConfigParser()
config.read('connection.cfg')

# Extract connection parameters
database = config['database']['dbname']
user = config['database']['user']
password = config['database']['password']
host = config['database']['host']
port = config['database']['port']

# Function to convert dataframe columns for apartments_sold
def process_apartments_sold(df):
    df.rename(columns={
        'floorCount': 'floors_number',
        'buildYear': 'built_year'
    }, inplace=True)
    df.drop(columns=['id', 'type'], inplace=True)

# Function to convert dataframe columns for job_offers
def process_job_offers(df):
    df.rename(columns={
        'company size': 'company_size',
        'salary employment min': 'emp_salary_min',
        'salary employment max': 'emp_salary_max',
        'salary b2b min': 'b2b_salary_min',
        'salary b2b max': 'b2b_salary_max',
    }, inplace=True)
    df.drop(columns=['id'], inplace=True)

# Function to convert dataframe columns for cities_data
def process_cities_data(df):
    df.rename(columns={
        'Nazwa': 'city_name',
        'Liczba Gospodarstw': 'residential_buildings',
        'Liczba transakcji': 'sold_apartments_per_year',
        'Średnia': 'average_apartment_price_1m2',
        'Mediana': 'median_apartment_price_1m2',
        'wolne miejsca pracy[tysiąc]': 'vacancies',
        'przeciętne wynagrodzenie': 'average_salary'
    }, inplace=True)
    df.drop(columns=[col for col in df.columns if col != 'id' and col not in df.columns[:1]], inplace=True)

# Process the dataframes
process_apartments_sold(apartments_sold)
process_job_offers(jobs_offers)
process_cities_data(residential_buildings)  # Assuming the dataframe for cities data is residential_buildings

# Function to insert dataframe into PostgreSQL
def insert_dataframe_to_db(conn, df, table_name):
    df.to_sql(table_name, conn, if_exists='append', index=False)

# Connect to PostgreSQL
try:
    conn = psycopg2.connect(
        dbname=database,
        user=user,
        password=password,
        host=host,
        port=port
    )

    # Insert dataframes into the corresponding tables
    process_apartments_sold(apartments_sold)
    insert_dataframe_to_db(conn, apartments_sold, 'Apartment_prices')

    process_job_offers(jobs_offers)
    insert_dataframe_to_db(conn, jobs_offers, 'Sofware_jobs')

    process_cities_data(residential_buildings)
    insert_dataframe_to_db(conn, residential_buildings, 'Cities')

    print("Data inserted successfully into the database.")

except Exception as e:
    print(f"An error occurred while inserting data: {e}")

finally:
    if conn:
        conn.close()


# **Notes:**
# - Ensure that the DataFrame names (apartments_sold, jobs_offers, and residential_buildings) are defined and cleaned as needed before this script runs.
# - Replace the placeholder for the `insert_dataframe_to_db` function with the actual method to read and insert data into PostgreSQL if needed, as `to_sql` is typically used with SQLAlchemy.
# - This code snippet directly addresses the column renaming and skips as specified in your requirements.