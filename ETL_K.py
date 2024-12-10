import pandas as pd
import numpy as np
import os
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

def set_first_as_columns(df):
    # df.columns = df.iloc[0]
    print(df.columns)
    df = df.drop(0)
    df = df.reset_index(drop=True)
    return df

def add_year_company(filename, df):
    file_name_without_extension = os.path.splitext(filename)[0]
    names = file_name_without_extension.split("_")
    company = names[0]
    year = names[1]
    code = names[2]
    df['rok'] = year
    df['firma'] = company
    df['Kod_S'] = df['Kod_S'].str.replace('.', '', regex=False)
    return df
def ETL_k(folder_name):
    df_S020102 = pd.DataFrame()
    df_S050102 = pd.DataFrame()
    df_S190121 = pd.DataFrame()
    df_S230101 = pd.DataFrame()
    path = os.getcwd() + '\\' + folder_name
    i = 0
    for filename in os.listdir(path):
        i += 1
        print(i)
        if filename.endswith('.csv'):
            print(filename)
            file_name_without_extension = os.path.splitext(filename)[0]
            names = file_name_without_extension.split("_")
            company = names[0]
            year = names[1]
            code = names[2]
            df = pd.read_csv(path + '\\' + filename)
            processed_df = add_year_company(filename, df)
            if type(processed_df) == pd.DataFrame:
                if code == 'S020102':
                    df_S020102 = pd.concat([df_S020102, processed_df], ignore_index=True)
                elif code == 'S050102':
                    df_S050102 = pd.concat([df_S050102, processed_df], ignore_index=True)
                elif code == 'S190121':
                    df_S190121 = pd.concat([df_S190121, processed_df], ignore_index=True)
                elif code == 'S230101':
                    df_S230101 = pd.concat([df_S230101, processed_df], ignore_index=True)
                else:
                    print("STH WRONG")
                    break
    df_S020102.to_csv('S020102_k.csv', index=False)
    df_S050102.to_csv('S050102_k.csv', index=False)
    df_S190121.to_csv('S190121_k.csv', index=False)
    df_S230101.to_csv('S230101_k.csv', index=False)


def merge_sources(df1, df2, final):
    df_combined = pd.concat([df1, df2], ignore_index=True)
    df_combined.to_csv(final, index=False)
    return df_combined


# dataframe = pd.read_csv('DANE_KK\\Allianz_2018_S020102.csv')
# # dataframe = set_first_as_columns(dataframe)
# # print(dataframe)
# dataframe = add_year_company('Allianz_2018_S020102', dataframe)
# print(dataframe)

print(merge_sources(pd.read_csv('S230101_k.csv'), pd.read_csv('S230101_m.csv'), 'S230101_final.csv'))