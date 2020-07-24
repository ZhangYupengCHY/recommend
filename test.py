import pandas as pd



path = r"C:\Users\Administrator\Desktop\utf8_ad_data2020072101-603.csv"
file_data = pd.read_csv(path, sep=',', error_bad_lines=False,warn_bad_lines=False)
print(file_data.columns)
print(file_data.head(2))
print(file_data.head(2))

