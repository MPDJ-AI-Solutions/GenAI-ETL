import pandas as pd
import glob

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

if __name__ == '__main__':
    apartments_prices = load_apartments_prices()
    jobs_offers = load_jobs_offers()
    city_data = load_city_data()

    print("Apartments Prices Data Frame")
    print(apartments_prices.shape)

    print("Jobs Offers Data Frame")
    print(jobs_offers.shape)

    print("Residential Buildings Data Frame")
    print(city_data['residential_buildings'].shape)

    print("Sold Apartments Data Frame")
    print(city_data['sold_apartments'].shape)

    print("Median Prices Data Frame")
    print(city_data['median_prices'].shape)

    print("Mean Prices Data Frame")
    print(city_data['mean_prices'].shape)

    print("Vacancies Data Frame")
    print(city_data['vacancies'].shape)

    print("Average Salary Data Frame")
    print(city_data['average_salary'].shape)
