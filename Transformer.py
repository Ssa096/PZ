import os

import pdfplumber
import pandas as pd
import pymupdf
import re


def process_and_merge_tables(sheet):

    # Wczytanie pliku
    # sheet = pd.read_excel(file_path, header=None, engine='openpyxl')  # Pierwszy arkusz, bez nagłówków

    tables = []
    current_table = []
    sheet = sheet.astype(str).replace(["", " ", "nan", "None"], pd.NA)

    # Iteracja przez wiersze
    for _, row in sheet.iterrows():
        if row.isnull().all():  # Sprawdzenie, czy wiersz jest pusty
            empty_row_count += 1  # Zwiększ licznik pustych wierszy
            if empty_row_count == 3:  # Jeśli mamy 3 puste wiersze
                if current_table:  # Jeśli wiersze zostały zebrane
                    tables.append(pd.DataFrame(current_table))
                    current_table = []
                empty_row_count = 0  # Zresetuj licznik pustych wierszy
        else:
            empty_row_count = 0  # Reset licznika, bo wiersz nie jest pusty
            current_table.append(row.values)



    # Dodanie ostatniej tabeli

    if current_table:
        tables.append(pd.DataFrame(current_table))


    # Scalanie tabel o różnej liczbie kolumn
    merged_tables = []
    for table in tables:
        table = table.dropna(axis=1, how='all')
        table = table.dropna(axis=0, how='all')
        table = table.replace({'\n': ' '}, regex=True)
        table = clean_number_columns(table)
        # print(table)
        if not merged_tables:  # Pierwsza tabela
            merged_tables.append(table)
        else:
            last_table = merged_tables[-1]
            if table.shape[1] == last_table.shape[1]:  # Liczba kolumn się zgadza
                merged_tables[-1] = pd.concat([last_table, table], ignore_index=True)
            else:
                merged_tables.append(table)

    # Ustawienie nagłówków i czyszczenie tabel
    final_tables = []
    for table in merged_tables:
        table = table.dropna(how='all', axis=1)  # Usuń puste kolumny
        table.columns = table.iloc[0]  # Pierwszy wiersz jako nagłówek
        table = table[1:].reset_index(drop=True)  # Usuń wiersz nagłówka z danych
        final_tables.append(table)

    return final_tables


def clean_number_columns(df):

    '''
    Funkcja usuwająca pojawiające sie numeracje stron przedstawione jako kolumne ((( ErgoHestia usable )))
    :param df:
    :return df:

    '''
    # Przechodzimy przez każdą kolumnę
    for column in df.columns:
        # Sprawdzamy, czy wszystkie wartości w kolumnie są <NA> z wyjątkiem ostatniego wiersza
        if df[column].iloc[:-1].isna().all() and isinstance(df[column].iloc[-1], str) and df[column].iloc[-1].isdigit():
            # Usuwamy kolumnę, jeśli warunki są spełnione
            df = df.drop(columns=[column])

    return df