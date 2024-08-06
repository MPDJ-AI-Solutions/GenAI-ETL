import pandas as pd
import glob

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
      'average_salary': f"{data_path}/WYNA_2497_CTAB_*"
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
    print("Apartments Prices DataFrame:\n", apartments_prices.shape)
    print("Jobs Offers DataFrame:\n", jobs_offers.shape)
    print("Residential Buildings DataFrame:\n", city_data['residential_buildings'].shape)