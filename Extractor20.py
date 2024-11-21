import os
from heapq import merge
from os import remove

import pdfplumber
import pandas as pd
import pymupdf
import re
import Transformer
from Recognizer import find_S190121
from Transformer import process_and_merge_tables, delete_needless_rows, split_by_regex, clean_tables_headers, \
    merge_tables_col, remove_empty_rows
import Recognizer

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


for file in os.listdir(os.getcwd()):
    if file != 'Generali_2023.pdf':
        continue
    if file[len(file)-3:len(file)] != 'pdf':
        continue

    doc = pymupdf.open(file)
    first = 0
    num = 1
    # sprawdzenie od której strony zaczynają się tabelki
    for i in range(1, 5):
        page = doc.load_page(i)
        text = str(page.get_text().encode("utf8"))
        match = re.search(r'(\d+)(?!.*\d)', text)
        if match:
            if first < int(match.group(1)) < len(doc):
                first = int(match.group(1))
    doc.close()
    print(first)

    with pdfplumber.open(file) as pdf:
        #  #################################################
        #  ####### Skrypt do wyciągania stricte tabel ######
        #  #################################################

        # dane = []
        # # Przejdź przez każdą stronę PDF
        #
        # for page in pdf.pages[first:]:
        #
        #     # Wyciągnij tabele z bieżącej strony
        #     tabela = page.extract_table()
        #     if tabela:
        #         dane.extend(tabela)
        # df = pd.DataFrame(dane[0:])  # Pierwszy wiersz jako nagłówki kolumn ### ODKLIKNĄĆ
         #################################################
         ####### Skrypt do wyciągania tekstu #############
         #################################################
        # all_text = []
        # for page in pdf.pages[first:]:
        #     text = page.extract_text()  # Wyciągnij tekst z każdej strony
        #     all_text.append(text)
        # rows = [line for page_text in all_text for line in page_text.split('\t')]
        # df = pd.DataFrame(rows, columns=["Tekst"])

        #  #################################################
        #  ####### Skrypt do wyciągania stricte tabel v2.0 ######
        #  #################################################

        dane = []
        # #Przejdź przez każdą stronę PDF

        for page in pdf.pages[first:]:

            # Wyciągnij tabele z bieżącej strony
            tabela = page.extract_table({
            "vertical_strategy": "lines",  # Sposób wykrywania pionowych linii tabeli
            # "horizontal_strategy": "lines",  # Sposób wykrywania poziomych linii tabeli
            "snap_tolerance": 4,  # Odstęp tolerancji, aby rozpoznać elementy w pobliżu linii
            "intersection_tolerance": 35,  # Tolerancja na przecięcia
            })
            if tabela:
                dane.extend(tabela)
                dane.extend([[""] * len(tabela[0])] * 3)
        df = pd.DataFrame(dane[0:])  # Pierwszy wiersz jako nagłówki kolumn ### ODKLIKNĄĆ


    new_file_name = file.replace(".pdf", ".xlsx")

# print(df)



tables = process_and_merge_tables(df)
tables = split_by_regex(tables)
tables = merge_tables_col(tables)
for i in range(len(tables)):
    print('#####')
    print(i)
    print('#####')
    print(tables[i])
    print('#####')
    # print('#####')
    # print(znajdz_i_usun_nad(tables[i]))

S190121_table = Recognizer.find_S190121_by_rows(df)


S190121_table.to_csv('Done2/Generali_2023_S190121.csv')


print(Recognizer.find_S020102(tables))
print(Recognizer.find_S050102(tables))
# print(Recognizer.find_S190121_by_rows(tables))
print(Recognizer.find_S230101(tables))

# Recognizer.find_S020102(tables).to_csv('Done2/Generali_2023_S020102.csv')
# Recognizer.find_S050102(tables).to_csv('Done2/Generali_2023_S050102.csv')
# # Recognizer.find_S190121(tables).to_csv('Done2/Generali_2023_S190121.csv')
# Recognizer.find_S230101(tables).to_csv('Done2/Generali_2023_S230101.csv')



