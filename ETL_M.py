import pandas as pd
import numpy as np
import os


COLUMN_S020102 = ['', '', '', 'C0010']
COLUMN_S050102 = ['', '', '', 'C0010', 'C0020', 'C0030', 'C0040', 'C0050', 'C0060', 'C0070',
                'C0080', 'C0090', 'C0100','C0110', 'C0120', 'C0130', 'C0140', 'C0150', 'C0160', 'C0200']
COLUMN_S190121 = ['', '', '', 'C0010', 'C0020', 'C0030', 'C0040', 'C0050', 'C0060', 'C0070',
                'C0080', 'C0090', 'C0100']
COLUMN_S230101 = ['', '', '', 'C0010', 'C0020', 'C0030', 'C0040', 'C0050']

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)


def ETL(folder_name):
    '''
    Funkcja do załadowania plików do bazy danych.
    :param folder_name: ścieżka do folderu z raportami w formacie .csv
    :return:
    '''
    df_S020102 = pd.DataFrame()
    df_S050102 = pd.DataFrame()
    df_S190121 = pd.DataFrame()
    df_S230101 = pd.DataFrame()
    path = os.getcwd() + '\\' + folder_name
    i = 0
    for filename in os.listdir(path):
        i+=1
        print(i)
        if filename.endswith('.csv'):
            print(filename)
            file_name_without_extension = os.path.splitext(filename)[0]
            names = file_name_without_extension.split("_")
            company = names[0]
            year = names[1]
            code = names[2]
            df = pd.read_csv(path + '\\' + filename)
            processed_df = preprocess_dataframe(filename, df)
            if type(processed_df) == pd.DataFrame:
                if code=='S020102':
                    df_S020102 = pd.concat([df_S020102, processed_df], ignore_index=True)
                elif code=='S050102':
                    df_S050102 = pd.concat([df_S050102, processed_df], ignore_index=True)
                elif code=='S190121':
                    df_S190121 = pd.concat([df_S190121, processed_df], ignore_index=True)
                elif code=='S230101':
                    df_S230101 = pd.concat([df_S230101, processed_df], ignore_index=True)
                else:
                    print("STH WRONG")
                    break
    df_S020102.to_csv('S020102_m.csv', index=False)
    df_S050102.to_csv('S050102_m.csv', index=False)
    df_S190121.to_csv('S190121_m.csv', index=False)
    df_S230101.to_csv('S230101_m.csv', index=False)

    return


def remove_first_row_if_matches(df, value_list):
    """
    Funkcja usuwa pierwszy wiersz DataFrame, jeśli jego wartości
    odpowiadają wartościom w podanej liście.

    :param df: DataFrame, z którego usuwamy wiersz
    :param value_list: Lista wartości, do której porównujemy pierwszy wiersz
    :return: Zaktualizowany DataFrame
    """
    # Sprawdzamy, czy pierwszy wiersz (df.iloc[0]) odpowiada liście value_list
    if df.iloc[0].tolist() == value_list:
        df = df.drop(index=0).reset_index(drop=True)
        print("Pierwszy wiersz został usunięty.")
    else:
        print("Pierwszy wiersz nie odpowiada podanej liście wartości.")

    return df

def check_column_match(df, txt_data):
    matching_rows = []
    for column in df.columns:
        column_data = df[column].astype(str).tolist()
        common_values = [value for value in txt_data if value in column_data]
        if set(txt_data).issubset(set(column_data)):
            matching_rows.append((column, common_values))
    if not matching_rows:
        return False
    return df

def clear_second_column(df):
    pattern = r'^R\d{4}$'
    filtered_df = df[df.iloc[:, 2].astype(str).str.match(pattern, na=False)]
    return filtered_df

def preprocess_dataframe(filename, dataframe):
    '''
    Function for loading SFCR data from csv files.
    It will check whether all standarized columns are in given dataframe based on its code.
    :return: Dataframe
    '''
    file_name_without_extension = os.path.splitext(filename)[0]
    names = file_name_without_extension.split("_")
    company = names[0]
    year = names[1]
    code = names[2]
    if code == 'S020102':
        with open('S020102_columns.txt', 'r') as file:
            txt_columns = file.read().splitlines()
        if type(check_column_match(dataframe, txt_columns)) != pd.DataFrame:
            print(False)
        dataframe = remove_first_row_if_matches(dataframe,COLUMN_S020102)
        dataframe.columns = ['ID', 'Nazwa', 'Kod_R', 'C0010']
        dataframe['rok'] = year
        dataframe['firma'] = company
        dataframe = dataframe[dataframe.iloc[:, 2] != 0].reset_index(drop=True)
        dataframe = clear_second_column(dataframe)
        dataframe = make_melted(dataframe)
        dataframe['Kod_S'] = code
        dataframe.drop(columns=['ID'], inplace=True)
        return dataframe

    elif code == 'S050102':
        with open('S050102_columns.txt', 'r') as file:
            txt_columns = file.read().splitlines()
        if type(check_column_match(dataframe, txt_columns)) != pd.DataFrame:
            print(False)
        dataframe = dataframe.fillna(0)
        dataframe = remove_first_row_if_matches(dataframe, COLUMN_S050102)
        dataframe.columns = ['ID', 'description', 'code', 'C0010', 'C0020', 'C0030', 'C0040', 'C0050', 'C0060', 'C0070',
                              'C0080', 'C0090', 'C0100','C0110', 'C0120', 'C0130', 'C0140', 'C0150', 'C0160', 'C0200']
        dataframe['rok'] = year
        dataframe['firma'] = company
        dataframe = dataframe[dataframe.iloc[:, 2] != 0].reset_index(drop=True)
        dataframe = clear_second_column(dataframe)
        dataframe = make_melted(dataframe)
        dataframe['Kod_S'] = code
        dataframe.drop(columns=['ID'], inplace=True)
        return dataframe

    elif code == 'S190121':
        with open('S190121_columns.txt', 'r') as file:
            txt_columns = file.read().splitlines()
        if type(check_column_match(dataframe, txt_columns)) != pd.DataFrame:
            print(False)
        dataframe = dataframe.fillna(0)
        dataframe = remove_first_row_if_matches(dataframe, COLUMN_S190121)
        dataframe.columns = ['ID', 'description', 'code', 'C0010', 'C0020', 'C0030', 'C0040', 'C0050', 'C0060', 'C0070',
                              'C0080', 'C0090', 'C0100']
        dataframe['rok'] = year
        dataframe['firma'] = company
        dataframe = dataframe[dataframe.iloc[:, 2] != 0].reset_index(drop=True)
        dataframe = clear_second_column(dataframe)
        dataframe = make_melted(dataframe)
        dataframe['Kod_S'] = code
        dataframe.drop(columns=['ID'], inplace=True)
        return dataframe

    elif code == 'S230101':
        with open('S230101_columns.txt', 'r') as file:
            txt_columns = file.read().splitlines()
        if type(check_column_match(dataframe, txt_columns)) != pd.DataFrame:
            print(False)
        dataframe = dataframe.fillna(0)
        dataframe = remove_first_row_if_matches(dataframe, COLUMN_S230101)
        dataframe.columns = ['ID', 'description', 'code', 'C0010', 'C0020', 'C0030', 'C0040', 'C0050']
        dataframe['rok'] = year
        dataframe['firma'] = company
        dataframe = dataframe[dataframe.iloc[:, 2] != 0].reset_index(drop=True)
        dataframe = clear_second_column(dataframe)
        dataframe = make_melted(dataframe)
        dataframe['Kod_S'] = code
        dataframe.drop(columns=['ID'], inplace=True)
        return dataframe

    else:
        return None

def make_melted(df):
    columns_to_melt = ['C0010', 'C0020', 'C0030', 'C0040', 'C0050', 'C0060', 'C0070','C0080',
                       'C0090', 'C0100','C0110', 'C0120', 'C0130', 'C0140', 'C0150', 'C0160', 'C0200']
    columns_to_melt_existing = [col for col in columns_to_melt if col in df.columns]
    print(columns_to_melt_existing)
    id_columns = [col for col in df.columns if col not in columns_to_melt_existing]
    print(id_columns)
    df_melted = df.melt(id_vars=id_columns, value_vars=columns_to_melt_existing,
                        var_name='Kod_C', value_name='Wartosc')
    return df_melted
# column_to_save = pd.read_csv('Done3\\Generali_2019_S230101.csv').iloc[:, 2]
# output_path = 'S230101_columns.txt'
# column_to_save.to_csv(output_path, index=False, header=False)

#
# preprocess_dataframe('Generali_2019_S230101.csv', pd.read_csv('Done3\\Generali_2019_S230101.csv'))
# print(pd.read_csv('Done3\\Generali_2019_S050102.csv'))
ETL('Done3')






