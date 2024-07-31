import pandas as pd

data = pd.read_csv("../Data/GUS/WYNA_2497_CTAB_20240731220629.csv", sep=';')

print(data.head())