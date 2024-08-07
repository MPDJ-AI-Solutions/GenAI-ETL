import pandas as pd
import numpy as np
import glob
import os

# Function to map 'yes'/'no' to True/False
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

# Print info about loaded data
print("Apartments data shape:", apartments_prices.shape)
print("Jobs offers data shape:", jobs_offers.shape)
print("Residential buildings data shape:", residential_buildings.shape)
print("Apartments sold data shape:", apartments_sold.shape)
print("Median prices data shape:", median_prices.shape)
print("Mean prices data shape:", mean_prices.shape)
print("Vacancies data shape:", vacancies.shape)
print("Average salary data shape:", avg_salary.shape)

# List of dataframes to remove 'Kod' column from
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

print("Data processing completed.")

print(city_data_combined)
print(apartments_prices)
print(jobs_offers)