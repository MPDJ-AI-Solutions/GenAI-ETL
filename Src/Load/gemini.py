import pandas as pd
import psycopg2

def store_dataframes(apartments_sold, job_offers, cities_data):
    """Stores the given dataframes into the specified database tables.

    Args:
        apartments_sold (pd.DataFrame): DataFrame containing apartment sales data.
        job_offers (pd.DataFrame): DataFrame containing job offer data.
        cities_data (pd.DataFrame): DataFrame containing city data.
    """

    # Database connection parameters (replace with your credentials)
    conn_params = {
        'host': 'your_host',
        'database': 'your_database',
        'user': 'your_user',
        'password': 'your_password'
    }

    # Connect to the database
    conn = psycopg2.connect(**conn_params)
    cur = conn.cursor()

    # Store apartments_sold
    apartments_sold = apartments_sold.rename(columns={'floorCount': 'floor_number'})
    apartments_sold = apartments_sold.drop(columns=['id'])  # Assuming 'id' is the column name

    # Convert DataFrame to list of tuples for efficient insertion
    data = [tuple(x) for x in apartments_sold.values]

    # Insert data into the database
    sql = """
        INSERT INTO apartments_sold (
            # Replace with actual column names
        )
        VALUES (%s, %s, %s, ...)
    """
    cur.executemany(sql, data)

    # Store job_offers
    job_offers = job_offers.rename(columns={
        'salary employment min': 'emp_salary_min',
        'salary employment max': 'emp_salary_max',
        'salary b2b min': 'b2b_salary_min',
        'salary b2b max': 'b2b_salary_max'
    })

    # Convert DataFrame to list of tuples for efficient insertion
    data = [tuple(x) for x in job_offers.values]

    # Insert data into the database
    sql = """
        INSERT INTO job_offers (
            # Replace with actual column names
        )
        VALUES (%s, %s, %s, ...)
    """
    cur.executemany(sql, data)

    # Store cities_data
    cities_data = cities_data.rename(columns={
        'Nazwa': 'city_name',
        'Liczba Gospodarstw': 'residential_buildings',
        'Liczba transakcji': 'sold_apartmetns_per_year',
        'Mediana': 'median_apartment_price_1m2',
        'wolne miejsca pracy[tysiąc]': 'vacancies',
        'przeciętne wynagrodzenie': 'average_salary'
    })

    # Calculate average apartment price
    cities_data['average_apartment_price_1m2'] = (cities_data['Średnia rynek pierwotny'] + cities_data['Średnia rynek wtórny']) / 2

    # Drop unnecessary columns
    cities_data = cities_data.drop(columns=['Średnia rynek pierwotny', 'Średnia rynek wtórny'])

    # Convert DataFrame to list of tuples for efficient insertion
    data = [tuple(x) for x in cities_data.values]

    # Insert data into the database
    sql = """
        INSERT INTO cities_data (
            # Replace with actual column names
        )
        VALUES (%s, %s, %s, ...)
    """
    cur.executemany(sql, data)

    # Commit changes and close connection
    conn.commit()
    cur.close()
    conn.close()

# Example usage:
# Assuming you have loaded the dataframes as apartments_sold, job_offers, and cities_data


store_dataframes(apartments_sold, job_offers, cities_data)