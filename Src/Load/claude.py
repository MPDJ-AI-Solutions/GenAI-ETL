import configparser
import os
import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_batch
import re
import glob
import math

def replace_nan(value):
    if isinstance(value, float) and math.isnan(value):
        return None
    return value

# Database connection parameters
def load_params():
    config = configparser.ConfigParser()
    config.read('./Src/Database/claude_connection.cfg')

    return  {
        'dbname': config['PostgreSQL']['database'],
        'user':  config['PostgreSQL']['user'],
        'password': config['PostgreSQL']['password'],
        'host':  config['PostgreSQL']['host'],
        'port': config['PostgreSQL']['port']
    }

def camel_to_snake(name):
    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()

def insert_dataframe(conn, df, table_name, column_mappings=None, skip_id=False):
    cursor = conn.cursor()
    
    if column_mappings:
        df = df.rename(columns=column_mappings)
    
    columns = df.columns.tolist()
    # FIX Change NaN to None - values = [tuple(x) for x in df.values]
    values = [tuple(replace_nan(value) for value in row) for row in df.values]

    insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
        sql.Identifier(table_name),
        sql.SQL(', ').join(map(sql.Identifier, columns)),
        sql.SQL(', ').join(sql.Placeholder() * len(columns))
    )
    
    cursor.executemany(insert_query, values)
    conn.commit()
    cursor.close()


def map_yes_no_to_bool(df, columns):
    for col in columns:
        df[col] = df[col].map({'yes': True, 'no': False})
    return df

# Function to remove prefix from city names
def remove_prefix(df, column, prefix):
    df[column] = df[column].str.replace(prefix, '', regex=False)
    return df

# Function to map voivodeship names to capital cities
def map_voivodeship_to_capital(df):
    voivodeship_capital_map = {
        'DOLNOŚLĄSKIE': 'Wrocław',
        'KUJAWSKO-POMORSKIE': 'Bydgoszcz',
        'LUBELSKIE': 'Lublin',
        'LUBUSKIE': 'Gorzów Wielkopolski',
        'ŁÓDZKIE': 'Łódź',
        'MAŁOPOLSKIE': 'Kraków',
        'MAZOWIECKIE': 'Warszawa',
        'OPOLSKIE': 'Opole',
        'PODKARPACKIE': 'Rzeszów',
        'PODLASKIE': 'Białystok',
        'POMORSKIE': 'Gdańsk',
        'ŚLĄSKIE': 'Katowice',
        'ŚWIĘTOKRZYSKIE': 'Kielce',
        'WARMIŃSKO-MAZURSKIE': 'Olsztyn',
        'WIELKOPOLSKIE': 'Poznań',
        'ZACHODNIOPOMORSKIE': 'Szczecin'
    }
    df['Nazwa'] = df['Nazwa'].map(voivodeship_capital_map)
    return df

# Set the data directory
data_dir = './Data/'

# Load apartments data
apartments_files = sorted(glob.glob(os.path.join(data_dir, 'apartments_pl_*.csv')))
 # FIX: change semicolon to comma - apartments_prices = pd.concat([pd.read_csv(f, sep=';') for f in apartments_files], ignore_index=True)
apartments_prices = pd.concat([pd.read_csv(f, sep=',') for f in apartments_files], ignore_index=True)

# Load jobs data
jobs_files = sorted(glob.glob(os.path.join(data_dir, '*_soft_eng_jobs_pol.csv')))
# FIX: change semicolon to comma - jobs_offers = pd.concat([pd.read_csv(f, sep=';') for f in jobs_files], ignore_index=True)
jobs_offers = pd.concat([pd.read_csv(f, sep=',') for f in jobs_files], ignore_index=True)

# Load cities data
residential_buildings = pd.read_csv(os.path.join(data_dir, 'GOSP_2909_CTAB_20240731215208.csv'), sep=';')
apartments_sold = pd.read_csv(os.path.join(data_dir, 'RYNE_3783_CTAB_20240731215821.csv'), sep=';')
median_prices = pd.read_csv(os.path.join(data_dir, 'RYNE_3787_CTAB_20240731215911.csv'), sep=';')
mean_prices = pd.read_csv(os.path.join(data_dir, 'RYNE_3788_CTAB_20240731215943.csv'), sep=';')
vacancies = pd.read_csv(os.path.join(data_dir, 'RYNE_4294_CTAB_20240731220115.csv'), sep=';')
avg_salary = pd.read_csv(os.path.join(data_dir, 'WYNA_2497_CTAB_20240731220629.csv'), sep=';')

dfs_to_remove_kod = [apartments_sold, median_prices, mean_prices, vacancies, avg_salary]

# Remove 'Kod' column
for df in dfs_to_remove_kod:
    if 'Kod' in df.columns:
        df.drop('Kod', axis=1, inplace=True)

# Map 'yes' and 'no' to True and False in apartment_prices
boolean_columns = ['hasParkingSpace', 'hasBalcony', 'hasElevator', 'hasSecurity', 'hasStorageRoom']
for col in boolean_columns:
    if col in apartments_prices.columns:
        apartments_prices[col] = apartments_prices[col].map({'yes': True, 'no': False})

# Define city data operations
city_data_ops = {
    'residential_buildings': lambda df: df.assign(Nazwa=df['Nazwa'].str.replace(' (1)', '')),
    'apartments_sold': lambda df: df.assign(Nazwa=df['Nazwa'].str.replace('Powiat m. ', '')),
    'median_prices': lambda df: df.assign(Nazwa=df['Nazwa'].str.replace('Powiat m. ', '')),
    'mean_prices': lambda df: df.assign(Nazwa=df['Nazwa'].str.replace('Powiat m. ', '')),
    'vacancies': lambda df: df.assign(Nazwa=df['Nazwa'].map({
        'DOLNOŚLĄSKIE': 'Wrocław',
        'KUJAWSKO-POMORSKIE': 'Bydgoszcz',
        'LUBELSKIE': 'Lublin',
        'LUBUSKIE': 'Gorzów Wielkopolski',
        'ŁÓDZKIE': 'Łódź',
        'MAŁOPOLSKIE': 'Kraków',
        'MAZOWIECKIE': 'Warszawa',
        'OPOLSKIE': 'Opole',
        'PODKARPACKIE': 'Rzeszów',
        'PODLASKIE': 'Białystok',
        'POMORSKIE': 'Gdańsk',
        'ŚLĄSKIE': 'Katowice',
        'ŚWIĘTOKRZYSKIE': 'Kielce',
        'WARMIŃSKO-MAZURSKIE': 'Olsztyn',
        'WIELKOPOLSKIE': 'Poznań',
        'ZACHODNIOPOMORSKIE': 'Szczecin'
    })),
    'average_salary': lambda df: df.assign(Nazwa=df['Nazwa'].str.replace('Powiat m. ', ''))
}

# Apply operations to city data
city_data = {}
for key, df in zip(['residential_buildings', 'apartments_sold', 'median_prices', 'mean_prices', 'vacancies', 'average_salary'],
                   [residential_buildings, apartments_sold, median_prices, mean_prices, vacancies, avg_salary]):
    city_data[key] = city_data_ops[key](df)

# Join city data dataframes
city_data_combined = city_data['residential_buildings']
for key in city_data:
    if key != 'residential_buildings':
        city_data_combined = city_data_combined.merge(city_data[key], on='Nazwa', how='outer')

# Rename columns
apartments_prices['city'] = apartments_prices['city'].str.capitalize()
apartments_prices.rename(columns={'city': 'city_id'}, inplace=True)
jobs_offers.rename(columns={'location': 'city_id'}, inplace=True)

# Create a mapping of city names to IDs
city_id_mapping = dict(zip(city_data_combined['Nazwa'], city_data_combined['Kod']))

# Map city names to IDs in apartments_prices and jobs_offers
apartments_prices['city_id'] = apartments_prices['city_id'].map(city_id_mapping)
jobs_offers['city_id'] = jobs_offers['city_id'].map(city_id_mapping)


# Connect to the database
conn = psycopg2.connect(**load_params())

# Store apartments_prices
apartments_prices_mappings = {
    'cityId': 'city_id',
    'squareMeters': 'square_meters',
    'rooms': 'rooms',
    'floorCount': 'floors_number',
    'floor': 'floor',
    'buildYear': 'built_year',
    'latitude': 'latitude',
    'longitude': 'longitude',
    'centreDistance': 'centre_distance',
    'poiCount': 'poi_count',
    'schoolDistance': 'school_distance',
    'clinicDistance': 'clinic_distance',
    'postOfficeDistance': 'post_office_distance',
    'kindergartenDistance': 'kindergarten_distance',
    'restaurantDistance': 'restaurant_distance',
    'collegeDistance': 'college_distance',
    'pharmacyDistance': 'pharmacy_distance',
    'ownership': 'ownership',
    'buildingMaterial': 'building_material',
    'condition': 'condition',
    'hasParkingSpace': 'has_parking_space',
    'hasBalcony': 'has_balcony',
    'hasElevator': 'has_elevator',
    'hasSecurity': 'has_security',
    'hasStorageRoom': 'has_storage_room',
    'price': 'price'
}

# Rename columns and convert to snake_case
apartments_prices = apartments_prices.rename(columns=apartments_prices_mappings)
# FIX Remove id column instead of values without id - apartments_prices = apartments_prices.dropna(columns=['id'])
apartments_prices = apartments_prices.drop(columns=['id', 'type'])
apartments_prices.columns = [camel_to_snake(col) for col in apartments_prices.columns]


# Store jobs_offers
jobs_offers_mappings = {
    'cityId': 'city_id',
    'company': 'company',
    'company size': 'company_size',
    'technology': 'technology',
    'seniority': 'seniority',
    'salary employment min': 'emp_salary_min',
    'salary employment max': 'emp_salary_max',
    'salary b2b min': 'b2b_salary_min',
    'salary b2b max': 'b2b_salary_max'
}

jobs_offers = jobs_offers.rename(columns=jobs_offers_mappings)
jobs_offers = jobs_offers.drop(columns=['id'])

# Store cities_data
cities_data_mappings = {
    'Kod': 'id',
    'Nazwa': 'city_name',
    'Liczba Gospodarstw': 'residential_buildings',
    'Liczba transakcji': 'sold_apartments_per_year',
    'Średnia': 'average_apartment_price_1m2',
    'Mediana': 'median_apartment_price_1m2',
    'wolne miejsca pracy[tysiąc]': 'vacancies',
    'przeciętne wynagrodzenie': 'average_salary'
}

cities_data = city_data_combined.rename(columns=cities_data_mappings)
cities_data = cities_data[list(cities_data_mappings.values())]

# FIX added skipping cities without id
cities_data = cities_data.dropna(subset=['id'])

insert_dataframe(conn, cities_data, 'Cities')
print("Cities data inserted.")
insert_dataframe(conn, jobs_offers, 'Software_jobs')
print("Job offers data inserted.")
insert_dataframe(conn, apartments_prices, 'Apartment_prices')
print("Apartment prices data inserted.")


# Close the database connection
conn.close()
print("Database connection closed.")