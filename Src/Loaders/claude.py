import pandas as pd
import glob
import os

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