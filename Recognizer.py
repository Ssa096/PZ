import pandas as pd
from Transformer import remove_empty_rows, delete_needless_rows, remove_rows_without_code


def find_S020102(tables):
    """
    Szuka tabeli w liście na podstawie:
    1. Nazwy w pierwszej kolumnie.
    2. Kodu w drugiej kolumnie.
    3. Nazwy kolumny w tabeli.

    :param tables: Lista DataFrame'ów.
    :param name: Wartość w pierwszej kolumnie, która ma być wyszukana.
    :param R_code: Wartość w drugiej kolumnie, która ma być wyszukana.
    :param C_code: Nazwa kolumny w tabeli.
    :return: Znaleziona tabela (DataFrame) lub None, jeśli nie znaleziono.
    """
    name = 'Aktywa z tytułu odroczonego podatku dochodowego'
    R_code = 'R0040'
    C_code = 'C0010'
    # name = name.lower()

    for index, df in enumerate(tables):
        # Sprawdzanie nazw kolumn z uwzględnieniem wielkości liter
        if C_code in df.columns:
            # Sprawdzanie nazwy w pierwszej kolumnie (ignorując wielkość liter)
            first_column = df.iloc[:, 0].apply(lambda x: str(x).lower() if isinstance(x, str) else x)

            # Sprawdzanie kodu w drugiej kolumnie (uwzględniając wielkość liter)
            second_column = df.iloc[:, 1] if len(df.columns) > 1 else []

            first_column_filtered = first_column.dropna()
            second_column_filtered = second_column.dropna()
            if name.lower() in first_column_filtered.values and R_code in second_column_filtered.values:
                df = extract_data(df, 'R0030', 'R1000', 'C0010')
            return remove_rows_without_code(df)
    return None

def find_S050102(tables):
    """
    Szuka tabeli w liście na podstawie:
    1. Nazwy w pierwszej kolumnie.
    2. Kodu w drugiej kolumnie.
    3. Nazwy kolumny w tabeli.

    :param tables: Lista DataFrame'ów.
    :param name: Wartość w pierwszej kolumnie, która ma być wyszukana.
    :param R_code: Wartość w drugiej kolumnie, która ma być wyszukana.
    :param C_code: Nazwa kolumny w tabeli.
    :return: Znaleziona tabela (DataFrame) lub None, jeśli nie znaleziono.
    """
    # name = 'Brutto – Reasekuracja czynna proporcjonalna'
    R_code = 'R0120'
    C_code = 'C0010'
    keywords = ["Brutto", "Reasekuracja", "proporcjonalna"]

    for index, df in enumerate(tables):
        # Sprawdzanie nazw kolumn z uwzględnieniem wielkości liter
        if C_code in df.columns:
            # Sprawdzanie nazwy w pierwszej kolumnie (ignorując wielkość liter)
            first_column = df.iloc[:, 0].apply(lambda x: str(x).lower() if isinstance(x, str) else x)

            # Sprawdzanie kodu w drugiej kolumnie (uwzględniając wielkość liter)
            second_column = df.iloc[:, 1] if len(df.columns) > 1 else []

            first_column_filtered = first_column.dropna()
            second_column_filtered = second_column.dropna()

            # Funkcja sprawdzająca obecność wszystkich słów kluczowych
            def contains_keywords(value):
                if isinstance(value, str):
                    value_lower = value.lower()
                    return all(keyword.lower() in value_lower for keyword in keywords)
                return False

            # Sprawdzanie, czy w pierwszej kolumnie znajdują się słowa kluczowe
            if any(first_column_filtered.apply(contains_keywords)) and R_code in second_column_filtered.values:
                df = extract_data(df, 'R0110', 'R1300', 'C0200')
                return remove_rows_without_code(df)
    return None

def find_S190121(tables):
    """
    Szuka tabeli w liście na podstawie:
    1. Nazwy w pierwszej kolumnie.
    2. Kodu w drugiej kolumnie.
    3. Nazwy kolumny w tabeli.

    :param tables: Lista DataFrame'ów.
    :param name: Wartość w pierwszej kolumnie, która ma być wyszukana.
    :param R_code: Wartość w drugiej kolumnie, która ma być wyszukana.
    :param C_code: Nazwa kolumny w tabeli.
    :return: Znaleziona tabela (DataFrame) lub None, jeśli nie znaleziono.
    """
    name_1 = 'N-9'
    name_2 = 'n–9'
    R_code = 'R0160'
    C_code = 'C0010'

    for index, df in enumerate(tables):

        if C_code in df.columns:
            first_column = df.iloc[:, 0].apply(lambda x: str(x).lower() if isinstance(x, str) else x)
            second_column = df.iloc[:, 1] if len(df.columns) > 1 else []

            first_column_filtered = first_column.dropna()
            second_column_filtered = second_column.dropna()

            if (
                    name_1.lower() in first_column_filtered.values or name_2.lower() in first_column_filtered.values) and R_code in second_column_filtered.values:
                df = extract_data(df, 'R0100', 'R0250', 'C0100')
                return remove_rows_without_code(df)


    return None

def find_S230101(tables):
    """
    Szuka tabeli w liście na podstawie:
    1. Nazwy w pierwszej kolumnie.
    2. Kodu w drugiej kolumnie.
    3. Nazwy kolumny w tabeli.

    :param tables: Lista DataFrame'ów.
    :param name: Wartość w pierwszej kolumnie, która ma być wyszukana.
    :param R_code: Wartość w drugiej kolumnie, która ma być wyszukana.
    :param C_code: Nazwa kolumny w tabeli.
    :return: Znaleziona tabela (DataFrame) lub None, jeśli nie znaleziono.
    """
    name = 'Akcje uprzywilejowane'
    R_code = 'R0090'
    C_code = 'C0010'


    for index, df in enumerate(tables):
        # Sprawdzanie nazw kolumn z uwzględnieniem wielkości liter
        if C_code in df.columns:
            # Sprawdzanie nazwy w pierwszej kolumnie (ignorując wielkość liter)
            first_column = df.iloc[:, 0].apply(lambda x: str(x).lower() if isinstance(x, str) else x)

            # Sprawdzanie kodu w drugiej kolumnie (uwzględniając wielkość liter)
            second_column = df.iloc[:, 1] if len(df.columns) > 1 else []

            first_column_filtered = first_column.dropna()
            second_column_filtered = second_column.dropna()
            if name.lower() in first_column_filtered.values and R_code in second_column_filtered.values:
                df = extract_data(df, 'R0010', 'R0640', 'C0050')
                return remove_rows_without_code(df)

    return None


def extract_data(df, row_start_value, row_end_value, column_end_name):
    """
    Funkcja wyciąga dane z DataFrame od wiersza zawierającego `row_start_value`
    do wiersza zawierającego `row_end_value`, a także od pierwszej kolumny do
    kolumny o nazwie `column_end_name`.

    :param df: DataFrame, z którego mają zostać wyciągnięte dane.
    :param row_start_value: Wartość w drugiej kolumnie, od której mają się zaczynać dane.
    :param row_end_value: Wartość w drugiej kolumnie, do której mają się kończyć dane.
    :param column_end_name: Nazwa kolumny, do której mają być wyciągnięte dane (łącznie z pierwszą kolumną).
    :return: Nowy DataFrame zawierający wyciągnięte dane.
    """

    # Sprawdzenie, czy nazwa kolumny istnieje
    if column_end_name not in df.columns:
        raise ValueError(f"Kolumna {column_end_name} nie istnieje w DataFrame")
    # Wyznaczenie indeksu początkowego i końcowego na podstawie wartości w drugiej kolumnie
    start_index = df[df.iloc[:, 1] == row_start_value].index[0]
    end_index = df[df.iloc[:, 1] == row_end_value].index[0]
    # Wyciąganie odpowiednich kolumn
    column_end_index = df.columns.get_loc(column_end_name) + 1  # +1, aby uwzględnić końcową kolumnę
    # Wyciągnięcie danych od start_index do end_index i od pierwszej kolumny do podanej nazwy kolumny
    result = df.iloc[start_index:end_index + 1, :column_end_index]

    return result


def find_S190121_by_rows(df, column_index=0, context_rows=3):
    """
    Znajduje wiersze w DataFrame, w których występuje określona sekwencja wartości
    w wybranej kolumnie, i zapisuje je do listy DataFrame'ów.

    Args:
        df (pd.DataFrame): DataFrame do przeszukania.
        column_index (int): Indeks kolumny, w której szukana jest sekwencja (domyślnie 0).
        context_rows (int): Liczba wierszy do pobrania przed początkiem sekwencji.
    Returns:
        list: Lista DataFrame'ów zawierających wiersze z sekwencją.
    """
    # Lista do przechowywania DataFrame'ów
    result_tables = []
    sequence = ['N-9', 'N-8', 'N-7', 'N-6', 'N-5', 'N-4', 'N-3', 'N-2', 'N-1', 'N']

    # Iterowanie przez wszystkie możliwe grupy w DataFrame
    for i in range(len(df) - len(sequence) + 1):
        # Wyodrębnienie potencjalnej sekwencji
        potential_sequence = df.iloc[i:i + len(sequence), column_index]
        # Sprawdzenie, czy sekwencja pasuje
        if list(potential_sequence) == sequence:
            # Ustal indeks początkowy z uwzględnieniem wierszy kontekstowych
            start_index = max(0, i - context_rows)
            end_index = i + len(sequence)
            # Dodanie odpowiedniego zakresu do wynikowej listy
            result_tables.append(remove_empty_rows(df.iloc[start_index:end_index].reset_index(drop=True)))
    final_tables = []
    for table in result_tables:
        final_tables.append(delete_needless_rows(table))
    return find_S190121(final_tables)