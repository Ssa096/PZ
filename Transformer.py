import os
from turtledemo.penrose import start
from typing import final

import pdfplumber
import pandas as pd
import pymupdf
import re


def process_and_merge_tables(sheet):

    # Wczytanie pliku
    # sheet = pd.read_excel(file_path, header=None, engine='openpyxl')  # Pierwszy arkusz, bez nagłówków

    tables = []
    current_table = []
    empty_row_count = 0

    sheet = sheet.astype(str).replace(["", " ", "nan", "None"], pd.NA)

    # Iteracja przez wiersze
    # Robimy to bo extractor wstawia 3 puste wiersze do .xlsx jako przerwe między tabele
    for index, row in sheet.iterrows():
        # Sprawdzanie, czy wiersz jest pusty
        if row.isnull().all():
            empty_row_count += 1  # Zwiększamy licznik pustych wierszy
            if empty_row_count == 3:  # Jeśli są 3 puste wiersze z rzędu
                if current_table:  # Jeśli są zgromadzone wiersze
                    tables.append(pd.DataFrame(current_table))
                    current_table = []
                empty_row_count = 0  # Reset licznika pustych wierszy
        else:
            empty_row_count = 0  # Reset licznika, bo wiersz nie jest pusty

            # Sprawdzenie wzorca ^C\d{3,}$
            if any(re.match(r'^C\d{3,}$', str(cell)) for cell in row if pd.notna(cell)):
                if current_table:  # Jeśli mamy już wiersze
                    tables.append(pd.DataFrame(current_table))
                    current_table = []

            # Dodanie wiersza do aktualnej tabeli
            current_table.append(row.values)

    # Dodanie ostatniej tabeli
    if current_table:
        tables.append(pd.DataFrame(current_table))

    tables = [table.reset_index(drop=True) for table in tables if len(table) > 0]

    # Scalanie tabel o różnej liczbie kolumn
    merged_tables = []
    for table in tables:
        table = table.dropna(axis=1, how='all')
        table = table.dropna(axis=0, how='all')
        table = table.replace({'\n': ' '}, regex=True)
        table = clean_number_columns(table)
        table = usun_polnaglowki(table)
        # print(table)
        if not merged_tables:  # Pierwsza tabela
            merged_tables.append(table)
        else:
            last_table = merged_tables[-1]
            if table.shape[1] == last_table.shape[1]:  # Liczba kolumn się zgadza
                merged_tables[-1] = pd.concat([last_table, table], ignore_index=True)
            else:
                merged_tables.append(table)
    #     # print('####')
    #     # print(table)
    #     # print('####')

    # Ustawienie nagłówków i czyszczenie tabel
    final_tables = []
    for table in merged_tables:
        if len(table) > 1:
            table = table.dropna(how='all', axis=1)  # Usuń puste kolumny
            table.columns = table.iloc[0]  # Pierwszy wiersz jako nagłówek
            table = table[1:].reset_index(drop=True)  # Usuń wiersz nagłówka z danych
            table.reset_index(level=None, drop=False, inplace=False, col_level=0, col_fill="")
            table = delete_needless_rows(table)
            final_tables.append(table)

    final_tables = clean_tables_headers(final_tables)
    final_tables = merge_divided(final_tables)
    # final_tables = merge_tables_cols(final_tables)
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


def merge_divided(list_tabel):
    """
    Funkcja łączy sąsiednie tabele, które mają tę samą liczbę wierszy
    i w których w pierwszej kolumnie nie ma samych stringów.

    ((( Generalli 2023 )))

    :param list_tabel: Lista tabel w postaci pandas.DataFrame
    :return: Zaktualizowana lista tabel
    """
    wynik = []
    i = 0

    while i < len(list_tabel):
        # Aktualna tabela
        tabela1 = list_tabel[i]

        # Sprawdzenie, czy jest następna tabela
        if i + 1 < len(list_tabel):
            tabela2 = list_tabel[i + 1]

            # Kryteria łączenia tabel
            if (
                    len(tabela1) == len(tabela2)  # Ta sama liczba wierszy
                    and not tabela1.iloc[:, 0].apply(lambda x: isinstance(x, str)).all()
            # Pierwsza kolumna nie jest samymi stringami
            ):
                # Łączenie tabel kolumnowo
                tabela_polaczona = pd.concat([tabela1, tabela2], axis=1)
                wynik.append(tabela_polaczona)
                i += 2  # Przeskocz dwie tabele
                continue

        # Jeśli nie ma łączenia, dodaj oryginalną tabelę
        wynik.append(tabela1)
        i += 1

    return wynik


def clean_tables_headers(list_tabel):
    """
    Funkcja oczyszcza listę tabel. Dla każdej tabeli usuwa wiersze
    przed tymi, które zawierają komórkę pasującą do wzorca 'C' + 3+ cyfry.

    :param list_tabel: Lista tabel w postaci pandas.DataFrame
    :return: Lista oczyszczonych tabel
    """

    def find_regex(tabela):
        """
        Znajduje pierwszy wiersz zawierający komórkę pasującą do wzorca 'C\d{3,}'
        :param tabela: pandas.DataFrame
        :return: Indeks pierwszego wiersza lub None
        """
        for i, row in tabela.iterrows():
            if any(
                re.match(r'C\d{3,}', str(cell).replace(" ", ""))  # Usuwanie spacji w komórkach
                for cell in row
                if pd.notna(cell)  # Ignorowanie NA
            ):
                return i
        return None

    oczyszczone_tabele = []

    for tabela in list_tabel:
        # print(tabela)
        # print(type(tabela))
        if len(tabela) > 0:
            # Znajdź indeks pierwszego wiersza spełniającego warunek
            start_idx = find_regex(tabela)
            print(start_idx)
            if start_idx is not None:
                # Usuń wiersze przed znalezionym indeksem
                oczyszczona_tabela = tabela.iloc[start_idx:].reset_index(drop=True)
                oczyszczone_tabele.append(oczyszczona_tabela)
            else:
                # Jeśli nie znaleziono wzorca, zwróć tabelę bez zmian
                oczyszczone_tabele.append(tabela)

    return oczyszczone_tabele

def delete_needless_rows(tabela):
    """
    Znajduje pierwszy wiersz w tabeli, który zawiera tylko wartości pd.NA
    oraz przynajmniej jedną komórkę pasującą do wzorca 'C' + 3+ cyfry.
    Usuwa wszystkie wiersze nad nim. Następnie ustawi wiersz z wzorcem jako kolumne tabeli wynikowej.

    :param tabela: pandas.DataFrame
    :return: pandas.DataFrame po przetworzeniu
    """

    def czy_wiersz_zgodny(row):
        """
        Sprawdza, czy wiersz zawiera tylko wartości pd.NA i kod 'C' + 3+ cyfry.
        """
        for cell in row:
            if pd.notna(cell):  # Jeśli nie jest NA
                if not re.match(r'^C\d{3,}$', str(cell)):  # Jeśli nie pasuje do wzorca
                    return False
        # Jeśli wiersz zawiera przynajmniej jedno dopasowanie do wzorca
        return any(re.match(r'^C\d{3,}$', str(cell)) for cell in row if pd.notna(cell))

    # Znajdź indeks pierwszego wiersza spełniającego warunek
    for i, row in tabela.iterrows():
        if czy_wiersz_zgodny(row):
            # Ustaw wiersz jako nagłówki kolumn
            tabela = tabela.iloc[i + 1:].reset_index(drop=True)  # Usuń wiersze powyżej i sam nagłówek
            tabela.columns = [str(cell) if pd.notna(cell) else f"Unnamed_{j}"
                              for j, cell in enumerate(row)]  # Ustaw nagłówki
            return tabela

    # Jeśli żaden wiersz nie spełnia warunku, zwróć tabelę bez zmian
    return tabela

def usun_polnaglowki(tabela):
    """
    Usuwa wiersze, gdzie w pierwszej kolumnie jest wartość typu str,
    a w pozostałych kolumnach są wyłącznie pd.NA.

    :param tabela: pandas.DataFrame
    :return: pandas.DataFrame po przetworzeniu
    """

    def czy_wiersz_pasuje(row):
        # Sprawdź, czy pierwsza komórka jest str
        pierwsza_kolumna = row.iloc[0]
        if isinstance(pierwsza_kolumna, str):
            # Sprawdź, czy reszta kolumn to wyłącznie pd.NA
            reszta = row.iloc[1:]
            return all(pd.isna(cell) for cell in reszta)
        return False

    # Usuń wiersze spełniające warunek
    return tabela[~tabela.apply(czy_wiersz_pasuje, axis=1)].reset_index(drop=True)

def split_by_regex(tables):
    """
    Rozdziela tabele w liście na podstawie wystąpienia wierszy zawierających tylko pd.NA
    i kod postaci 'C\d{3,}'.

    :param tables: Lista DataFrame'ów.
    :return: Lista DataFrame'ów po podziale.
    """
    wynikowe_tabele = []

    for tabela in tables:
        podzial_punkty = tabela.apply(
            lambda row: all(
                pd.isna(cell) or re.match(r'^C\d{3,}$', str(cell)) for cell in row
            ),
            axis=1
        )

        # Znalezienie indeksów punktów podziału
        punkty_podzialu = tabela.index[podzial_punkty].tolist()

        # Jeśli nie ma punktów podziału, dodajemy tabelę bez zmian
        if not punkty_podzialu:
            wynikowe_tabele.append(tabela)
            continue

        # Dodanie tabeli od startu do punktów podziału
        start_idx = 0
        for punkt in punkty_podzialu:
            if start_idx < punkt:
                wynikowe_tabele.append(tabela.iloc[start_idx:punkt].reset_index(drop=True))
            start_idx = punkt

        # Dodanie końcowego fragmentu od ostatniego punktu podziału do końca tabeli
        wynikowe_tabele.append(tabela.iloc[start_idx:].reset_index(drop=True))

    return wynikowe_tabele

def merge_tables_col(tables):
    """
    Łączy dwa sąsiednie DataFrame'y w liście kolumnowo, jeśli mają taką samą liczbę wierszy.

    :param tables: Lista DataFrame'ów.
    :return: Lista DataFrame'ów po połączeniu sąsiednich, jeśli spełniają kryterium.
    """
    wynik = []
    skip_next = False  # Flaga do pominięcia już połączonych DataFrame'ów

    for i in range(len(tables) - 1):
        if skip_next:  # Pomijamy, jeśli poprzedni DataFrame został już połączony
            skip_next = False
            continue

        df1, df2 = tables[i], tables[i + 1]

        if len(df1) == len(df2):  # Sprawdzenie liczby wierszy
            # Łączenie kolumnowo
            polaczony = pd.concat([df1.reset_index(drop=True), df2.reset_index(drop=True)], axis=1)
            wynik.append(polaczony)
            skip_next = True  # Następny DataFrame został już połączony
        else:
            wynik.append(df1)

    # Dodanie ostatniego DataFrame'a, jeśli nie był połączony
    if not skip_next and tables:
        wynik.append(tables[-1])

    return wynik

def remove_empty_rows(df):
    """
    Usuwa puste wiersze z DataFrame.
    Pusty wiersz to taki, w którym wszystkie wartości to None lub ''.

    Args:
        df (pd.DataFrame): DataFrame do przetworzenia.

    Returns:
        pd.DataFrame: DataFrame bez pustych wierszy.
    """
    clean_df = df.replace('', pd.NA)
    return clean_df.dropna(how='all')
