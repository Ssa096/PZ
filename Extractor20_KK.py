import os

import pdfplumber
import pandas as pd
import pymupdf
import re

from matplotlib.pyplot import table

from Transformer_KK import process_and_merge_tables
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


for file in os.listdir(os.getcwd()):
    if file[len(file)-3:len(file)] != 'pdf':
        continue
    # if file != 'Warta_2022.pdf':
    #     continue

    doc = pymupdf.open(file)
    first = 0
    # sprawdzenie od której strony zaczynają się tabelki
    for i in range(1, 2):
        page = doc.load_page(i)
        text = str(page.get_text().encode("utf8"))
        match = re.search(r'(\d+)(?!.*\d)', text)
        if match:
            if first < int(match.group(1)) < len(doc):
                first = int(match.group(1))
    doc.close()

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
        firma = file[:len(file) - 4].split('_')[0]
        rok = file[:len(file) - 4].split('_')[1]  # Też do Warty
        match_prev = None # Tego potrzebuję do Warty
        for page in pdf.pages[first:]:
            # Wyciągnij tabele z bieżącej strony
            if firma == 'Allianz' and rok == '2022' and re.search('31-12-2021',page.extract_text()):
                # wywalam te dodatkowe, o których mówiłem
                break
            tabele = None
            if firma == 'Warta' and rok == '2022':
                tabele = page.extract_tables({"snap_tolerance": 4}) # Again, nielicząc Warty 2022 wszystko wczytuje się twoimi ustawieniami
            else:
                tabela = page.extract_table({
                "vertical_strategy": "lines",  # Sposób wykrywania pionowych linii tabeli
                "horizontal_strategy": "lines",  # Sposób wykrywania poziomych linii tabeli
                "snap_tolerance": 4,  # Odstęp tolerancji, aby rozpoznać elementy w pobliżu linii
                "intersection_tolerance": 35,  # Tolerancja na przecięcia
                })
            if tabele is not None: # Warta 2022 - tam używam extract_tables a nie extract_table, więc wyciągam pojedyncze tabele z listy
                tabela = []
                for elem in tabele:
                    tabela += elem
            if tabela:
                if not re.search(r'S\.\d{2}\.\d{2}\.\d{2}', str(tabela)): # dla Warty nie czyta kodów tabel, więc dodaje kod dodający takowy, jeśli go nie ma
                    match = re.search(r'S\.\d{2}\.\d{2}\.\d{2}', page.extract_text())
                    if match:
                        new_line = ["" for i in range(1, len(tabela[0]))]
                        new_line.insert(0, match.group(0))
                        tabela.insert(0, new_line)
                        match_prev = match
                    elif match_prev is not None:
                        new_line = ["" for i in range(1, len(tabela[0]))]
                        new_line.insert(0, match_prev.group(0))
                        tabela.insert(0, new_line)
                dane.extend(tabela)
                dane.extend([[""] * len(tabela[0])] * 3)
        df = pd.DataFrame(dane[0:])  # Pierwszy wiersz jako nagłówki kolumn ### ODKLIKNĄĆ
    # new_file_name = file.replace(".pdf", ".xlsx")

    print(file)
    tables = process_and_merge_tables(df)
    for i in range(len(tables)):
        code_s = tables[i].iloc[0, 0].replace('.', '')
        # sprawdzenie czy istnieje biblioteka do zapisu, jak nie to tworzę nową
        dirpath = os.path.join(os.getcwd(), "DANE_KK")
        if not os.path.exists(dirpath):
            os.makedirs(dirpath)
        path = os.path.join(dirpath, f"{firma}_{rok}_{code_s}.csv")
        if not (firma == 'Warta' and rok == '2022' and (code_s == 'S020102' or code_s == 'S050102')):
            tables[i].to_csv(path, index=False)