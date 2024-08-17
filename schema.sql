BEGIN;
CREATE TABLE IF NOT EXISTS public."Cities"
(
    id SERIAL,
    city_name text NOT NULL,
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
    id SERIAL,
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
    kindergarten_distance real,
    restaurant_distance real,
    college_distance real,
    pharmacy_distance real,
    ownership text,
    building_material text,
    condition text,
    has_parking_space boolean,
    has_balcony boolean,
    has_elevator boolean,
    has_security boolean,
    has_storage_room boolean,
    price integer NOT NULL,
    CONSTRAINT "Apartment_prices_pkey" PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public."Software_jobs"
(
    id SERIAL,
    city_id integer,
    company text,
    company_size real,
    technology text,
    seniority text,
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