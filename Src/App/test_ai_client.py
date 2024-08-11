from APIClient import AIClient

client = AIClient(True)

client.define_structure(
    """
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
        "has_security " boolean,
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
)

client.define_extract(
    """
    - load data about apartments prices from 11 .csv files named: apartments_pl_2023_08.csv – apartments_pl_2024_06 to data frame named apartments_prices
    - load data about jobs offers from 11 .csv files named 202309_soft_eng_jobs_pol.csv – 202407_soft_eng_jobs_pol.csv to data frame named jobs_offers
    - loads data about cities and store them in different data frames variables. Values are separated with semicolons ‘;’ and data is stored in files named and: 
        - GOSP_2909_CTAB_20240731215208.csv (Number of residential buildings in city) 
        - RYNE_3783_CTAB_20240731215821.csv (Number of apartments sold)
        - RYNE_3787_CTAB_20240731215911.csv (Median of 1m2 prices)
        - RYNE_3788_CTAB_20240731215943.csv (Mean of 1m2 prices)
        - RYNE_4294_CTAB_20240731220115.csv (Number of vacancies)
        - WYNA_2497_CTAB_20240731220629.csv (Average monthly salary)

    """
)

client.define_transform(
    """
    - Remove „Kod” column from dataframes: „apartments_sold”, “median_price”, “mean_price”, “vacancies”, “avg_salary”.
    - map string values “yes” and “no” to true and false in dataframe apartment_prices, in the following columns: hasParkingSpace, hasBalcony, hasElevator, hasSecurity, hasStorageRoom,
    - capitalize city names in apartment_price dataframe,
    - fix polish diacritical signs in city names in apartment_price dataframe
    - for given name of key in array of dataframes city_data do:
    -	“residental_buildings” – remove “ (1)” in column “Nazwa”.
    -	“apartments_sold” – remove “Powiat m. “ in column “Nazwa”.
    -	“median_price” – remove “Powiat m. “ in column “Nazwa”.
    -	“mean_price” – remove “Powiat m. “ in column “Nazwa”.
    -	“vacancies” – map name of polish voivodeship to name of voivodeships capital.
    -	“avg_salary” – remove “Powiat m. “ in column “Nazwa”.
    - join each dataframe of city_data array on column “Nazwa”.
    - rename column “city” to “city_id” in apartment_price.
    - rename column “location” to “city_id” in jobs_offers.
    - Map each “city_id” column of apartment_price and jobs_offers dataframes to corresponding id using values from columns “Kod” and “Nazwa” of city_data dataframe.
    """
)

client.define_load(
    """
    - Store df “apartmets_sold” in the corresponding table. To achieve this goal, change camelCase names to snake_case names, skip “id” and “type” and change following names:  

    floorCount -> floors_number, buildYear -> built_year


    - Store df “job_offers” in the corresponding table. To achieve this goal, skip “id” and change following names:  

    company size -> company_size, salary employment min -> emp_salary_min, salary employment max -> emp_salary_max, salary b2b min -> b2b_salary_min, salary b2b max -> b2b_salary_max,


    - Store df “cities_data” in the corresponding table. To achieve this goal, skip cities without “id” and change following names drop other values (leave id):  

    Nazwa -> city_name, Liczba Gospodarstw -> residential_buildings, Liczba transakcji -> sold_apartments_per_year, Średnia -> average_apartment_price_1m2, Mediana -> median_apartment_price_1m2, wolne miejsca pracy[tysiąc] -> vacancies, przeciętne wynagrodzenie -> average_salary  
    """
)

client.save_results("output.py")