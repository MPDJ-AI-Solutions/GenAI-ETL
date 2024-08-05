import psycopg2
from configparser import ConfigParser

def config(filename='./Src/Database/chatgpt_connection.cfg', section='database'):
    parser = ConfigParser()
    parser.read(filename)

    db_params = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db_params[param[0]] = param[1]
    else:
        raise Exception(f'Section {section} not found in the {filename} file')

    return db_params

def create_tables():
    commands = (
        """
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
        """,
        """
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
            "has_security " boolean,
            has_storage_room boolean,
            price integer NOT NULL,
            CONSTRAINT "Apartment_prices_pkey" PRIMARY KEY (id)
        );
        """,
        """
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
        """,
        """
        ALTER TABLE IF EXISTS public."Apartment_prices"
            ADD CONSTRAINT "City_ref" FOREIGN KEY (city_id)
            REFERENCES public."Cities" (id) MATCH SIMPLE
            ON UPDATE NO ACTION
            ON DELETE NO ACTION;
        """,
        """
        ALTER TABLE IF EXISTS public."Sofware_jobs"
            ADD CONSTRAINT "City_ref" FOREIGN KEY (city_id)
            REFERENCES public."Cities" (id) MATCH SIMPLE
            ON UPDATE NO ACTION
            ON DELETE NO ACTION
            NOT VALID;
        """
    )

    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        for command in commands:
            cur.execute(command)
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

if __name__ == '__main__':
    create_tables()