import os

import pdfplumber
import pandas as pd
import pymupdf
import re
import Transformer
from Transformer import process_and_merge_tables
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


for file in os.listdir(os.getcwd()):
    if file != 'ErgoHestia_2022.pdf':
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


tables = process_and_merge_tables(df)
for i in range(len(tables)):
    print('#####')
    print(i)
    print('#####')
    print(tables[i])



