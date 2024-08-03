import psycopg2

def create_database(db_name, user, password, host, port):
    """Creates a PostgreSQL database."""

    conn = psycopg2.connect(
        database="postgres",
        user=user,
        password=password,
        host=host,
        port=port
    )
    conn.autocommit = True
    cursor = conn.cursor()  


    cursor.execute(f"CREATE DATABASE {db_name}")
    cursor.close()
    conn.close()

def create_tables(db_name, user, password, host, port):
    """Creates tables in the specified PostgreSQL database."""

    conn = psycopg2.connect(
        database=db_name,
        user=user,
        password=password,
        host=host,
        port=port
    )
    conn.autocommit = True
    cursor = conn.cursor()

    sql_script = """
    CREATE TABLE IF NOT EXISTS
    public."Cities"
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

    CREATE TABLE IF NOT EXISTS
    public."Apartment_prices"
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
        "has_security " boolean,
        has_storage_room boolean,
        price integer NOT NULL,
        CONSTRAINT "Apartment_prices_pkey" PRIMARY KEY (id)
    );

    CREATE TABLE IF NOT EXISTS
    public."Sofware_jobs"
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

    ALTER TABLE IF EXISTS
    public."Apartment_prices"
        ADD CONSTRAINT "City_ref" FOREIGN KEY (city_id)
        REFERENCES public."Cities" (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

    ALTER TABLE IF EXISTS
    public."Sofware_jobs"
        ADD CONSTRAINT "City_ref" FOREIGN KEY (city_id)
        REFERENCES public."Cities" (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID;
    """

    cursor.execute(sql_script)
    cursor.close()
    conn.close()

if __name__ == "__main__":
    # Replace with your PostgreSQL credentials
    db_name = "Cities_Gemini"
    user = "gemini"
    password = "1234"
    host = "localhost"
    port = "5432"

    create_database(db_name, user, password, host, port)
    create_tables(db_name, user, password, host, port)