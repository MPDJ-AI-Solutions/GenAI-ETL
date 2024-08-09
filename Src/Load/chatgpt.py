import pandas as pd
import psycopg2
from psycopg2 import sql
import glob
import math

def replace_nan(value):
    if isinstance(value, float) and math.isnan(value):
        return None
    return value

def connect_to_db(config_file='./Src/Load/chatgpt_connection.cfg'):
    config = {}
    with open(config_file, 'r') as file:
        for line in file:
            name, value = line.strip().split('=')
            config[name] = value

    print("Load data to: ")
    for key in config:
        print(key + " " + config[key])

    conn = psycopg2.connect(
        dbname=config['dbname'],
        user=config['user'],
        password=config['password'],
        host=config['host'],
        port=config['port']
    )
    return conn

def clean_and_store_apartments_sold(apartments_sold, conn):
    # Change column names to snake_case
    apartments_sold.columns = apartments_sold.columns.str.replace('([A-Z])', r'_\1').str.lower()
    
    # Rename specific columns
    apartments_sold = apartments_sold.rename(columns={
        'floorcount': 'floor_number'
    })

    # Drop the 'id' column if it exists
    if 'id' in apartments_sold.columns:
        apartments_sold = apartments_sold.drop(columns=['id', 'type'])

    # Insert into database
    with conn.cursor() as cur:
        for _, row in apartments_sold.iterrows():
            cleaned_row = tuple(replace_nan(v) for v in row)
            cur.execute(
                sql.SQL("""
                    INSERT INTO public."Apartment_prices" (city_id, square_meters, rooms, floor, floors_number, built_year, latitude, longitude, centre_distance, poi_count, school_distance, clinic_distance, post_office_distance, kindergarden_distance, restaurant_distance, college_distance, pharmacy_distance, ownership, building_material, condition, has_parking_space, has_balcony, has_elevator, has_security, has_storage_room, price)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """),
                cleaned_row
            )
        conn.commit()

def clean_and_store_jobs_offers(jobs_offers, conn):
    # Rename specific columns
    jobs_offers = jobs_offers.rename(columns={
        'salary employment min': 'emp_salary_min',
        'salary employment max': 'emp_salary_max',
        'salary b2b min': 'b2b_salary_min',
        'salary b2b max': 'b2b_salary_max'
    })

    if 'id' in jobs_offers.columns:
        jobs_offers = jobs_offers.drop(columns=['id'])

    # Insert into database
    with conn.cursor() as cur:
        for _, row in jobs_offers.iterrows():
            cleaned_row = tuple(replace_nan(v) for v in row)
            cur.execute(
                sql.SQL("""
                    INSERT INTO public."Software_jobs" (company, company_size, city_id, technology, seniority, emp_salary_min, emp_salary_max, b2b_salary_min, b2b_salary_max)
                    VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """),
                cleaned_row
            )
        conn.commit()

def clean_and_store_cities_data(city_data_joined, conn):
    city_data_joined = city_data_joined[[
        'Kod', 'Nazwa', 'Liczba Gospodarstw', 'Liczba transakcji', 'Średnia', 'Mediana', 'wolne miejsca pracy[tysiąc]', 'przeciętne wynagrodzenie'
    ]]

    # Rename specific columns
    city_data_joined = city_data_joined.rename(columns={
        'Nazwa': 'city_name',
        'Średnia': "average_apartment_price_1m2",
        'Liczba Gospodarstw': 'residential_buildings',
        'Liczba transakcji': 'sold_apartments_per_year',
        'Mediana': 'median_apartment_price_1m2',
        'wolne miejsca pracy[tysiąc]': 'vacancies',
        'przeciętne wynagrodzenie': 'average_salary'
    })

    # Drop the 'id' column if it exists
    if 'id' in city_data_joined.columns:
        city_data_joined = city_data_joined.drop(columns=['id'])

    # Insert into database
    with conn.cursor() as cur:
        for _, row in city_data_joined.iterrows():
            cleaned_row = tuple(replace_nan(v) for v in row)
            if cleaned_row[0] == None: 
                continue

            cur.execute(
                sql.SQL("""
                    INSERT INTO public."Cities" (id, city_name, residential_buildings, sold_apartments_per_year, median_apartment_price_1m2, average_apartment_price_1m2, vacancies, average_salary)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """),
                cleaned_row
        
            )
        conn.commit()

def load_apartments_prices():
    csv_files = glob.glob('./Data/apartments_pl_2023_08.csv') + \
                glob.glob('./Data/apartments_pl_2023_09.csv') + \
                glob.glob('./Data/apartments_pl_2023_10.csv') + \
                glob.glob('./Data/apartments_pl_2023_11.csv') + \
                glob.glob('./Data/apartments_pl_2023_12.csv') + \
                glob.glob('./Data/apartments_pl_2024_01.csv') + \
                glob.glob('./Data/apartments_pl_2024_02.csv') + \
                glob.glob('./Data/apartments_pl_2024_03.csv') + \
                glob.glob('./Data/apartments_pl_2024_04.csv') + \
                glob.glob('./Data/apartments_pl_2024_05.csv') + \
                glob.glob('./Data/apartments_pl_2024_06.csv')
    df_list = [pd.read_csv(file) for file in csv_files]
    apartments_prices = pd.concat(df_list, ignore_index=True)
    return apartments_prices

def load_jobs_offers():
    csv_files = glob.glob('./Data/202309_soft_eng_jobs_pol.csv') + \
                glob.glob('./Data/202310_soft_eng_jobs_pol.csv') + \
                glob.glob('./Data/202311_soft_eng_jobs_pol.csv') + \
                glob.glob('./Data/202312_soft_eng_jobs_pol.csv') + \
                glob.glob('./Data/202401_soft_eng_jobs_pol.csv') + \
                glob.glob('./Data/202402_soft_eng_jobs_pol.csv') + \
                glob.glob('./Data/202403_soft_eng_jobs_pol.csv') + \
                glob.glob('./Data/202404_soft_eng_jobs_pol.csv') + \
                glob.glob('./Data/202405_soft_eng_jobs_pol.csv') + \
                glob.glob('./Data/202406_soft_eng_jobs_pol.csv') + \
                glob.glob('./Data/202407_soft_eng_jobs_pol.csv')
    df_list = [pd.read_csv(file) for file in csv_files]
    jobs_offers = pd.concat(df_list, ignore_index=True)
    return jobs_offers

def load_city_data():
    residential_buildings = pd.read_csv('./Data/GOSP_2909_CTAB_20240731215208.csv', delimiter=';')
    sold_apartments = pd.read_csv('./Data/RYNE_3783_CTAB_20240731215821.csv', delimiter=';')
    median_prices = pd.read_csv('./Data/RYNE_3787_CTAB_20240731215911.csv', delimiter=';')
    mean_prices = pd.read_csv('./Data/RYNE_3788_CTAB_20240731215943.csv', delimiter=';')
    vacancies = pd.read_csv('./Data/RYNE_4294_CTAB_20240731220115.csv', delimiter=';')
    average_salary = pd.read_csv('./Data/WYNA_2497_CTAB_20240731220629.csv', delimiter=';')
    
    return {
        'residential_buildings': residential_buildings,
        'sold_apartments': sold_apartments,
        'median_prices': median_prices,
        'mean_prices': mean_prices,
        'vacancies': vacancies,
        'average_salary': average_salary
    }

def clean_city_data(city_data):
    columns_to_remove = ['Kod']
    for key in ['sold_apartments', 'median_prices', 'mean_prices', 'vacancies', 'average_salary']:
        city_data[key] = city_data[key].drop(columns=columns_to_remove)
    
    return city_data

def map_apartment_prices_boolean_columns(apartment_prices):
    boolean_columns = ['hasParkingSpace', 'hasBalcony', 'hasElevator', 'hasSecurity', 'hasStorageRoom']
    for column in boolean_columns:
        apartment_prices[column] = apartment_prices[column].map({'yes': True, 'no': False})
    
    return apartment_prices

def clean_column_values(city_data):
    city_data['residential_buildings']['Nazwa'] = city_data['residential_buildings']['Nazwa'].str.replace(' (1)', '')
    city_data['sold_apartments']['Nazwa'] = city_data['sold_apartments']['Nazwa'].str.replace('Powiat m. ', '')
    city_data['median_prices']['Nazwa'] = city_data['median_prices']['Nazwa'].str.replace('Powiat m. ', '')
    city_data['mean_prices']['Nazwa'] = city_data['mean_prices']['Nazwa'].str.replace('Powiat m. ', '')
    city_data['average_salary']['Nazwa'] = city_data['average_salary']['Nazwa'].str.replace('Powiat m. ', '')

    voivodeships_to_capitals = {
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

    city_data['vacancies']['Nazwa'] = city_data['vacancies']['Nazwa'].map(voivodeships_to_capitals)
    
    return city_data

def join_city_data(city_data):
    city_data_joined = city_data['residential_buildings']
    for key in ['sold_apartments', 'median_prices', 'mean_prices', 'vacancies', 'average_salary']:
        city_data_joined = city_data_joined.merge(city_data[key], on='Nazwa', how='outer')
    
    return city_data_joined

def map_city_names_to_ids(apartment_prices, jobs_offers, city_data_joined):
    city_mapping = city_data_joined[['Kod', 'Nazwa']].set_index('Nazwa')['Kod'].to_dict()
    apartment_prices['city_id'] = apartment_prices['city_id'].map(city_mapping)
    jobs_offers['city_id'] = jobs_offers['city_id'].map(city_mapping)
    
    return apartment_prices, jobs_offers

if __name__ == '__main__':
    apartments_prices = load_apartments_prices()
    jobs_offers = load_jobs_offers()
    city_data = load_city_data()

    # Remove "Kod" column from specified dataframes
    city_data = clean_city_data(city_data)
    
    # Map "yes" and "no" to True and False in apartment_prices
    apartments_prices = map_apartment_prices_boolean_columns(apartments_prices)

    # Clean specific column values
    city_data = clean_column_values(city_data)

    # Join city data
    city_data_joined = join_city_data(city_data)

    # Rename columns
    apartments_prices['city'] = apartments_prices['city'].str.capitalize()
    apartments_prices = apartments_prices.rename(columns={'city': 'city_id'})
    jobs_offers = jobs_offers.rename(columns={'location': 'city_id'})

    # Map city names to IDs
    apartments_prices, jobs_offers = map_city_names_to_ids(apartments_prices, jobs_offers, city_data_joined)

    
    # Connect to the database
    conn = connect_to_db()

    # Clean and store dataframes
    clean_and_store_cities_data(city_data_joined, conn)
    clean_and_store_apartments_sold(apartments_prices, conn)
    clean_and_store_jobs_offers(jobs_offers, conn)

    # Close the connection
    conn.close()