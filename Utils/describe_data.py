import pandas as pd

def print_file_info(path):
    data = pd.read_csv(path)
    data.info()

print_file_info('./Data/apartments_pl_2023_08.csv')
print_file_info('./Data/RYNE_3783_CTAB_20240731215821.csv')
print_file_info('./Data/202309_soft_eng_jobs_pol.csv')
