from idlelib.pyparse import trans

import numpy as np
import pandas as pd
import re


def process_and_merge_tables(sheet):

    # Wczytanie pliku
    # sheet = pd.read_excel(file_path, header=None, engine='openpyxl')  # Pierwszy arkusz, bez nagłówków

    tables = []
    current_table = []
    sheet = sheet.astype(str).replace(["", " ", "nan", "None", '-'], pd.NA) # tu dorzuciłem myślnik jeszcze
    empty_row_count = 0
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
    i_ban = -1
    for i in range(len(merged_tables)):
        if i == i_ban: # w jednym miejscu łącze dwie tabele i tą drugą wywalam z obiegu
            continue
        table = merged_tables[i]
        table = table.dropna(how='all', axis=1)  # Usuń puste kolumny
        table.columns = table.iloc[0]  # Pierwszy wiersz jako nagłówek
        table = table[1:].reset_index(drop=True)  # Usuń wiersz nagłówka z danych
        if not table.empty and re.search(f'(S.02.01.02)|(S.05.01.02)|(S.19.01.21)|(S.23.01.01)|(S.22.01.21)', str(table)): # Wczytuję tylko niepuste zbiory oraz te które ustaliliśmy
            if re.search('S.05.01.02', str(table)) and re.search('C0210', str(table)) and not re.search('C0010', str(table)):
                continue # nie czytam tabel z kodem 5, które mają C powyżej 200
            if re.search('S.19.01.21', str(table)): # ,,wycinam'' trójkąt
                for j in range(len(table.columns)):
                    match = re.search(r'C\d{4}', np.array2string(table.iloc[:, j].values))
                    if match:
                        if int(match.group(0)[1:5]) == 110:
                            table = table.iloc[:, :(j+1)]
                            break
                for j in range(len(table)):
                    match = re.search(r'C\d{4}', np.array2string(table.iloc[[j]].values))
                    if match:
                        if int(match.group(0)[1:5]) > 110:
                            table = table.iloc[:j, :]
                            break
            if re.search('S.22.01.21', str(table)): # kod tutaj dla Allianza też wczytuje do tej tabeli tą 23, więc jej potrzebuje
                if len(re.findall('R0010', str(table))) > 1:
                    found_one = False
                    j = 0
                    while j < len(table):
                        if re.search('R0010', str(table.iloc[j])):
                            if not found_one: # pierwsze R0010 dotyczy 22 a drugie 23, więc szukam tego drugiego
                                found_one = True
                            else:
                                table = table.iloc[(j-2):, :]
                                j = 2
                        if re.search('R0640', str(table.iloc[j])):
                            table = table.iloc[:(j+1),:]
                        j += 1
                    table = table.reset_index(drop=True)
                    table.iloc[0, 0] = 'S.23.01.01' # tabela jest otagowana jako 22, więc muszę to dopisać
                else:
                    continue
            if re.search('S.23.01.01', str(table)) and not re.search('R0640', str(table)): # z kolei w Warcie są podzielone na dwie xd
                # czyszczenie tych dwóch tabel, żeby dało je się połączyć
                next_table = merged_tables[i+1]
                for j in range(len(next_table)):
                    if re.search('R0640', str(next_table.iloc[j])):
                        next_table = next_table.iloc[:(j+1),:]
                        break
                to_drop = []
                for j in range(len(next_table.columns)):
                    if not re.search(r'(S\.\d{2}\.\d{2}\.\d{2})|(R\d{4})|(C\d{4})',np.array2string(next_table.iloc[:,j].values)):
                        to_drop.append(next_table.columns[j])
                next_table = next_table.drop(to_drop, axis=1)
                next_table.columns = table.columns
                table = pd.concat([table, next_table], ignore_index=True)
                i_ban = i + 1 # tu otagowanie tej do wywalenia z obiegu
            elif re.search('S.23.01.01', str(table)):
                for j in range(len(table)):
                    if re.search('R0640', str(table.iloc[j])):
                        table = table.iloc[:(j+1),:]
                        break
            # finalny preprocessing
            s_code = re.findall(r'S\.\d{2}\.\d{2}\.\d{2}', str(table))[-1]
            for j in range(len(table)):
                if re.search(r'C\d{4}', str(table.iloc[j])):
                    table = table.iloc[j:, :]
                    break
            table = table.reset_index(drop=True)
            to_drop = []
            c = None
            for j in range(len(table)):
                # wywalam wiersze bez kodu R lub C
                if not re.search(r'(R\d{4})|(C\d{4})', np.array2string(table.iloc[j].values)):
                    to_drop.append(j)
                elif re.search(r'C\d{4}', np.array2string(table.iloc[j].values)):
                    # tutaj wywalam wiersze z kodem C ale tym samym co jest na górze tabeli
                    if c is None:
                        c = re.findall(r'C\d{4}', np.array2string(table.iloc[j].values))
                    elif re.findall(r'C\d{4}', np.array2string(table.iloc[j].values)) == c:
                        to_drop.append(j)
            table = table.drop(to_drop).dropna(how='all', axis=1).dropna(how='all', axis=0).reset_index(drop=True)
            if re.search('S.19.01.21', str(table)) and not re.search('N-9', str(table)): # allianz 2022 nie czyta pierwszej kolumny
                new_col = ['', 'Wcześniejsze lata', 'N-9', 'N-8', 'N-7', 'N-6', 'N-5', 'N-4', 'N-3', 'N-2', 'N-1', 'N']
                table.insert(0, 'cos', new_col)
            table.iloc[0, 0] = 'Nazwa'
            table.iloc[0, 1] = 'Kod'
            # finalna tabela
            fin_table = []
            for j in range(1, len(table)):
                for k in range(2, len(table.columns)):
                    if not pd.isna(table.iloc[j, k]):
                        fin_table.append([s_code, table.iloc[0, k], table.iloc[j, 1], table.iloc[j, 0], table.iloc[j, k]])
            table = pd.DataFrame(fin_table)
            table.columns = ['Kod_S', 'Kod_C', 'Kod_R', 'Nazwa', 'Wartosc']
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