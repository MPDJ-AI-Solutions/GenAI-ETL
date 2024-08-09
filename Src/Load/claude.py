import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_batch

# Database connection parameters
db_params = {
    'dbname': 'your_database_name',
    'user': 'your_username',
    'password': 'your_password',
    'host': 'localhost',
    'port': '5432'
}

def connect_to_db():
    return psycopg2.connect(**db_params)

def camel_to_snake(name):
    return ''.join(['_'+c.lower() if c.isupper() else c for c in name]).lstrip('_')

def insert_apartments(conn, df):
    df = df.rename(columns={col: camel_to_snake(col) for col in df.columns})
    df = df.rename(columns={'floor_count': 'floor_number'})
    
    columns = [col for col in df.columns if col != 'id']
    
    query = sql.SQL("INSERT INTO public.\"Apartment_prices\" ({}) VALUES ({})").format(
        sql.SQL(', ').join(map(sql.Identifier, columns)),
        sql.SQL(', ').join(sql.Placeholder() * len(columns))
    )
    
    with conn.cursor() as cur:
        execute_batch(cur, query, df[columns].values.tolist())
    
    conn.commit()

def insert_jobs(conn, df):
    df = df.rename(columns={
        'salary employment min': 'emp_salary_min',
        'salary employment max': 'emp_salary_max',
        'salary b2b min': 'b2b_salary_min',
        'salary b2b max': 'b2b_salary_max'
    })
    
    columns = df.columns
    
    query = sql.SQL("INSERT INTO public.\"Sofware_jobs\" ({}) VALUES ({})").format(
        sql.SQL(', ').join(map(sql.Identifier, columns)),
        sql.SQL(', ').join(sql.Placeholder() * len(columns))
    )
    
    with conn.cursor() as cur:
        execute_batch(cur, query, df.values.tolist())
    
    conn.commit()

def insert_cities(conn, df):
    df['average_apartment_price_1m2'] = df['Średnia ryniek pierwotny'] + df['Średnia rynek wtórny']
    
    df = df.rename(columns={
        'Nazwa': 'city_name',
        'Liczba Gospodarstw': 'residential_buildings',
        'Liczba transakcji': 'sold_apartments_per_year',
        'Mediana': 'median_apartment_price_1m2',
        'wolne miejsca pracy[tysiąc]': 'vacancies',
        'przeciętne wynagrodzenie': 'average_salary'
    })
    
    columns = [
        'city_name', 'residential_buildings', 'sold_apartments_per_year',
        'average_apartment_price_1m2', 'median_apartment_price_1m2',
        'vacancies', 'average_salary'
    ]
    
    query = sql.SQL("INSERT INTO public.\"Cities\" ({}) VALUES ({})").format(
        sql.SQL(', ').join(map(sql.Identifier, columns)),
        sql.SQL(', ').join(sql.Placeholder() * len(columns))
    )
    
    with conn.cursor() as cur:
        execute_batch(cur, query, df[columns].values.tolist())
    
    conn.commit()


if __name__ == "__main__":
    # Assuming you have already loaded your dataframes
    # apartments_prices, jobs_offers, cities_data
    
    conn = connect_to_db()
    
    try:
        insert_apartments(conn, apartments_prices)
        print("Apartment data inserted successfully.")
        
        insert_jobs(conn, jobs_offers)
        print("Job data inserted successfully.")
        
        insert_cities(conn, cities_data)
        print("City data inserted successfully.")
        
    except Exception as e:
        print(f"An error occurred: {e}")
        conn.rollback()
    
    finally:
        conn.close()