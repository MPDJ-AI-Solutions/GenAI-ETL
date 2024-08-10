import configparser
import glob
import math
import pandas as pd
import psycopg2

def replace_nan(value):
    if isinstance(value, float) and math.isnan(value):
        return None
    return value

def store_dataframes(cities_data, apartments_sold, job_offers, ):
    """Stores the given dataframes into the specified database tables.

    Args:
        apartments_sold (pd.DataFrame): DataFrame containing apartment sales data.
        job_offers (pd.DataFrame): DataFrame containing job offer data.
        cities_data (pd.DataFrame): DataFrame containing city data.
    """

    # Database connection parameters (replace with your credentials)
    config = configparser.ConfigParser()
    config.read('./Src/Database/gemini_connection.cfg')

    conn = psycopg2.connect(
        database=config['postgresql']['database'],
        user=config['postgresql']['user'],
        password=config['postgresql']['password'],
        host=config['postgresql']['address'],
        port=config['postgresql']['port']
    )
    cur = conn.cursor()

    cities_data = cities_data[[
        'Kod', 'Nazwa', 'Liczba Gospodarstw', 'Liczba transakcji', 'Średnia', 'Mediana', 'wolne miejsca pracy[tysiąc]', 'przeciętne wynagrodzenie'
    ]]

    # Store cities_data
    cities_data = cities_data.rename(columns={
        'Kod': 'id',
        'Nazwa': 'city_name',
        'Średnia': "average_apartment_price_1m2",
        'Liczba Gospodarstw': 'residential_buildings',
        'Liczba transakcji': 'sold_apartments_per_year',
        'Mediana': 'median_apartment_price_1m2',
        'wolne miejsca pracy[tysiąc]': 'vacancies',
        'przeciętne wynagrodzenie': 'average_salary'
    })

    # FIX - skip cities without ID
    cities_data = cities_data.dropna(subset=['id'])

    # Convert DataFrame to list of tuples for efficient insertion
    # FIX replace NaN - data = [tuple(x) for x in cities_data.value]
    data = [tuple(replace_nan(value) for value in row) for row in cities_data.values]

    # Insert data into the database
    # FIX not provided sql column names
    sql = """
        INSERT INTO public."Cities" (id, city_name, residential_buildings, sold_apartments_per_year, median_apartment_price_1m2, average_apartment_price_1m2, vacancies, average_salary)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    cur.executemany(sql, data)
    # Store apartments_sold
    apartments_sold = apartments_sold.rename(columns={'floorCount': 'floors_number'})
    if 'id' in apartments_sold.columns:
        apartments_sold = apartments_sold.drop('id', axis=1)  # Use axis=1 for columns
    if 'type' in apartments_sold.columns:
        apartments_sold = apartments_sold.drop('type', axis=1) # 'id' is the column name

    # Convert DataFrame to list of tuples for efficient insertion
    data = [tuple(x) for x in apartments_sold.values]

    # FIX replace NaN - data = [tuple(x) for x in apartments_sold.values]
    data = [tuple(replace_nan(value) for value in row) for row in apartments_sold.values]

    # Insert data into the database
    # FIX not provided sql column names
    sql = """
        INSERT INTO public."Apartment_prices" (city_id, square_meters, rooms, floor, floors_number, built_year, latitude, longitude, centre_distance, poi_count, school_distance, clinic_distance, post_office_distance, kindergarten_distance, restaurant_distance, college_distance, pharmacy_distance, ownership, building_material, condition, has_parking_space, has_balcony, has_elevator, has_security, has_storage_room, price)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cur.executemany(sql, data)

    # Store job_offers
    job_offers = job_offers.rename(columns={
        'company size': 'company_size', 
        'salary employment min': 'emp_salary_min',
        'salary employment max': 'emp_salary_max',
        'salary b2b min': 'b2b_salary_min',
        'salary b2b max': 'b2b_salary_max'
    })

    if 'id' in job_offers.columns:
        job_offers = job_offers.drop('id', axis=1)  # Use axis=1 for columns

    # Convert DataFrame to list of tuples for efficient insertion
    # FIX replace NaN - data = [tuple(x) for x in job_offers.values]
    data = [tuple(replace_nan(value) for value in row) for row in job_offers.values]
    # Insert data into the database
    # FIX not provided sql column names
    sql = """
        INSERT INTO public."Software_jobs" (company, company_size, city_id, technology, seniority, emp_salary_min, emp_salary_max, b2b_salary_min, b2b_salary_max)
        VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cur.executemany(sql, data)

    # Commit changes and close connection
    conn.commit()
    cur.close()
    conn.close()

# Example usage:
# Assuming you have loaded the dataframes as apartments_sold, job_offers, and cities_data
def preprocess_data(apartments_prices, jobs_offers, city_data):
    """
    Preprocesses dataframes for further analysis.

    Args:
        apartments_prices (pd.DataFrame): DataFrame containing apartment prices data.
        jobs_offers (pd.DataFrame): DataFrame containing job offers data.
        city_data (dict): Dictionary of DataFrames containing city data.

    Returns:
        tuple: Tuple of preprocessed DataFrames (apartments_prices, jobs_offers, city_data).
    """

    # Remove 'Kod' column from specific dataframes
    for df_name in ['apartments_sold', 'median_price', 'mean_price', 'vacancies', 'avg_salary']:
        city_data[df_name].drop(columns=['Kod'], inplace=True)

    # Map boolean values in apartment_prices
    boolean_columns = ['hasParkingSpace', 'hasBalcony', 'hasElevator', 'hasSecurity', 'hasStorageRoom']
    apartments_prices[boolean_columns] = apartments_prices[boolean_columns].replace({'yes': True, 'no': False})

    # Clean city data
    for key, df in city_data.items():
        if key == 'residential_buildings':
            df['Nazwa'] = df['Nazwa'].str.replace(' (1)', '')
        elif key in ['apartments_sold', 'median_price', 'mean_price', 'avg_salary']:
            df['Nazwa'] = df['Nazwa'].str.replace('Powiat m. ', '')
        elif key == 'vacancies':
            # Impleme nt mapping of voivodeship names to capitals (replace with your mapping logic)
            voivodeship_mapping = {
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
            df['Nazwa'] = df['Nazwa'].map(voivodeship_mapping)

    # Join city dataframes
    merged_city_data = city_data['residential_buildings']
    for key, df in city_data.items():
        if key != 'residential_buildings':
            merged_city_data = merged_city_data.merge(df, on='Nazwa', how='outer')

    # Rename city columns
    apartments_prices['city'] = apartments_prices['city'].str.capitalize()
    apartments_prices.rename(columns={'city': 'city_id'}, inplace=True)
    jobs_offers.rename(columns={'location': 'city_id'}, inplace=True)

    # Map city names to ids
    city_mapping = dict(zip(merged_city_data['Nazwa'], merged_city_data['Kod']))
    apartments_prices['city_id'] = apartments_prices['city_id'].map(city_mapping)
    jobs_offers['city_id'] = jobs_offers['city_id'].map(city_mapping)

    return apartments_prices, jobs_offers, merged_city_data

def load_apartment_prices(data_path):
  """Loads apartment price data from CSV files into a DataFrame."""
  file_pattern = f"{data_path}/apartments_pl_*"
  files = glob.glob(file_pattern)
  dfs = [pd.read_csv(file) for file in files]
  apartments_prices = pd.concat(dfs, ignore_index=True)
  return apartments_prices

def load_job_offers(data_path):
  """Loads job offer data from CSV files into a DataFrame."""
  file_pattern = f"{data_path}/*_soft_eng_jobs_pol.csv"
  files = glob.glob(file_pattern)
  dfs = [pd.read_csv(file) for file in files]
  jobs_offers = pd.concat(dfs, ignore_index=True)
  return jobs_offers

def load_city_data(data_path):
  """Loads city data from CSV files into separate DataFrames."""
  file_patterns = {
      'residential_buildings': f"{data_path}/GOSP_2909_CTAB_*",
      'apartments_sold': f"{data_path}/RYNE_3783_CTAB_*",
      'median_price': f"{data_path}/RYNE_3787_CTAB_*",
      'mean_price': f"{data_path}/RYNE_3788_CTAB_*",
      'vacancies': f"{data_path}/RYNE_4294_CTAB_*",
      'avg_salary': f"{data_path}/WYNA_2497_CTAB_*"
  }

  city_data = {}
  for key, pattern in file_patterns.items():
    file = glob.glob(pattern)[0]
    df = pd.read_csv(file, sep=';')
    city_data[key] = df

  return city_data


if __name__ == "__main__":
    data_path = "./Data"  # Replace with your data path

    apartments_prices = load_apartment_prices(data_path)
    jobs_offers = load_job_offers(data_path)
    city_data = load_city_data(data_path)

    # Access city data using:
    # print("Apartments Prices DataFrame:\n", apartments_prices.shape)
    # print("Jobs Offers DataFrame:\n", jobs_offers.shape)
    # print("Residential Buildings DataFrame:\n", city_data['residential_buildings'].shape)

    apartments_prices, jobs_offers, merged_city_data = preprocess_data(apartments_prices, jobs_offers, city_data)

    # print(merged_city_data)
    # print(apartments_prices)
    # print(jobs_offers)
    store_dataframes(merged_city_data, apartments_prices, jobs_offers)