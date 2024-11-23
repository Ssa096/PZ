import os
import pandas as pd
import pymupdf
import re

# to do wyświetlania tabelek, jak wszystko będzie działać to się te opcje wywali
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


for file in os.listdir(os.getcwd()):
    if file[len(file)-3:len(file)] == 'pdf':
    # if file == 'ErgoHestia_2023.pdf':
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
        # zamykam i otwieram na nowo, żeby nie używać kodowania utf-8
        doc = pymupdf.open(file)
        tables = []
        groups = []
        prev_group = ''
        # wczytanie tabel oraz ich grup (typu S.02.01.02 itd.), aby później prościej się pracowało
        for i in range(first, len(doc)):
            page = doc.load_page(i)
            tabs = page.find_tables()
            group = re.search('S\.\d{2}\.\d{2}\.\d{2}', page.get_text())
            if group:
                prev_group = group.group(0)
            for j in range(len(tabs.tables)):
                tables.append(tabs.tables[j].to_pandas())
                groups.append(prev_group)
        for i in range(len(tables)):
            # dodanie nazw kolumn jako dodatkowy wiersz, bo czasami tam są wartości liczbowe, które powinny być nie
            # jako nazwy kolumn
            new_row = list(tables[i].columns)
            for j in range(len(new_row)):
                new_row[j] = new_row[j][((j // 10) + 2):len(new_row[j])]
            tables[i].loc[-1] = new_row
            tables[i].index = tables[i].index + 1
            tables[i].sort_index(inplace=True)
            # sprawdzenie czy istnieje biblioteka do zapisu, jak nie to tworzę nową
            # dirpath = os.path.join(os.getcwd(), f"{file[0:len(file) - 4]}")
            # if not os.path.exists(dirpath):
            #     os.makedirs(dirpath)
            # path = os.path.join(dirpath, f"{file[0:len(file) - 4]}_{num}.csv")
            # df.to_csv(path, index=False)
            # num += 1
            for k in range(len(tables[i].columns)):
                # usnięcię znaków '\n'
                tables[i] = tables[i].rename(columns={tables[i].columns[k]:tables[i].columns[k].replace('\n', ' ')})
                for l in range(len(tables[i])):
                    if type(tables[i].iloc[l, k]) == str:
                        tables[i].iloc[l, k] = tables[i].iloc[l, k].replace('\n', ' ')
        # Tutaj zaczyna się transformacja tabel, najpierw sprawdzam grupę tabeli, te kolejne sprawdzenia to są takie
        # dyskretne sprawdzenia z jakiej firmy są tabele (z tym że jest szansa, że jak wleci totalnie nowa firma to
        # i tak ekstraktor sobie poradzi. Potem żmudny preprocessing (nazwy kolumn, wywalenie niepotrzebnych wierszy,
        # połaczenie/podzielenie tabel).
        for i in range(len(tables)):
            df = tables[i]
            if groups[i] == 'S.02.01.02':
                # to z jakiej firmy pochodzą sprawdzam na podstawie kodów zmiennych z R na początku
                if (df.where(df == 'R0010').notnull().sum().sum() == 0
                        and df.where(df == 'R0500').notnull().sum().sum() > 0
                        and df.where(df == 'R0900').notnull().sum().sum() > 0):
                    # S.02.01.02 - całość
                    df.columns = ['Nazwa', 'Kod', 'C0010']
                    to_drop = []
                    for k in range(len(df)):
                        if type(df.iloc[k, 1]) != str:
                            to_drop.append(k)
                        elif df.iloc[k, 1] == '':
                            to_drop.append(k)
                        elif df.iloc[k, 1][0] != 'R':
                            to_drop.append(k)
                    df = df.drop(to_drop).reset_index(drop=True)
                    groups[i] = ''
                else:
                    # S.02.01.02 - podzielone
                    df.columns = ['Nazwa', 'Kod', 'C0010']
                    to_drop = []
                    for k in range(len(df)):
                        # wywalam jeśli są puste
                        if type(df.iloc[k, 1]) != str:
                            to_drop.append(k)
                        # wywalam jeśli są puste (to samo ale jeśli to string)
                        elif df.iloc[k, 1] == '':
                            to_drop.append(k)
                        # wywalam jeśli są niepuste ale nie są kodem zmiennej zaczynającym się na R
                        elif df.iloc[k, 1][0] != 'R':
                            to_drop.append(k)
                    df = df.drop(to_drop)
                    groups[i] = ''
                    df_temp = tables[i+1]
                    df_temp.columns = ['Nazwa', 'Kod', 'C0010']
                    to_drop = []
                    for k in range(len(df_temp)):
                        if type(df_temp.iloc[k, 1]) != str:
                            to_drop.append(k)
                        elif df_temp.iloc[k, 1] == '':
                            to_drop.append(k)
                        elif df_temp.iloc[k, 1][0] != 'R':
                            to_drop.append(k)
                    df_temp = df_temp.drop(to_drop)
                    groups[i+1] = ''
                    df = pd.concat([df, df_temp]).reset_index(drop=True)
            elif groups[i] == 'S.05.01.02':
                if df.where(df == 'R1300').notnull().sum().sum() > 0 and df.where(df == 'R0110').notnull().sum().sum() > 0:
                    to_drop = []
                    for k in range(len(df)):
                        if type(df.iloc[k, 1]) != str:
                            to_drop.append(k)
                        elif df.iloc[k, 1] == '':
                            to_drop.append(k)
                        # tu jeszcze wywalam część zmiennych, jeśli domyślnie tabela ma mieć podzielona na więcej części
                        # dzielimy zgodnie z tym co jest w Solvency II od strony nr. 6
                        elif int(df.iloc[k, 1][1:5]) > 1300 or df.iloc[k, 1][0] != 'R':
                            to_drop.append(k)
                    df_temp = df.drop(to_drop).reset_index(drop=True)
                    df_temp.columns = ['Nazwa', 'Kod', 'C0010', 'C0020', 'C0030', 'C0040', 'C0050', 'C0060', 'C0070',
                                       'C0080', 'C0090', 'C0100', 'C0110', 'C0120', 'C0130', 'C0140', 'C0150',
                                       'C0160', 'C0200']
                    if df.where(df == 'R2700').notnull().sum().sum() > 0 and df.where(df == 'R1410').notnull().sum().sum() > 0:
                        to_drop = []
                        for k in range(len(df)):
                            if type(df.iloc[k, 1]) != str:
                                to_drop.append(k)
                            elif df.iloc[k, 1] == '':
                                to_drop.append(k)
                            elif int(df.iloc[k, 1][1:5]) <= 1300 or df.iloc[k, 1][0] != 'R':
                                to_drop.append(k)
                        df_temp = df.drop(to_drop).reset_index(drop=True)
                        for k in range(11, len(df.columns)):
                            df_temp = df_temp.drop(df.columns[k], axis=1)
                        df_temp.columns = ['Nazwa', 'Kod', 'C0210', 'C0220', 'C0230', 'C0240', 'C0250', 'C0260',
                                           'C0270', 'C0280', 'C0300']
                elif (df.where(df == 'R0300').notnull().sum().sum() > 0
                      and df.where(df == 'R0110').notnull().sum().sum() > 0
                      and df.where(df == 'C0010').notnull().sum().sum() > 0):
                    to_drop = []
                    for k in range(len(df)):
                        if type(df.iloc[k, 1]) != str:
                            to_drop.append(k)
                        elif df.iloc[k, 1] == '':
                            to_drop.append(k)
                        elif df.iloc[k, 1][0] != 'R':
                            to_drop.append(k)
                    df = df.drop(to_drop).reset_index(drop=True)
                    df_temp = tables[i + 2]
                    df_temp2 = tables[i + 1]
                    df_temp3 = tables[i + 3]
                    groups[i] = ''
                    groups[i+1] = ''
                    groups[i+2] = ''
                    groups[i+3] = ''
                    to_drop = []
                    for k in range(len(df_temp)):
                        if type(df_temp.iloc[k, 1]) != str:
                            to_drop.append(k)
                        elif df_temp.iloc[k, 1] == '':
                            to_drop.append(k)
                        elif df_temp.iloc[k, 1][0] != 'R':
                            to_drop.append(k)
                    df_temp = df_temp.drop(to_drop).reset_index(drop=True)
                    to_drop = []
                    for k in range(len(df_temp2)):
                        if type(df_temp2.iloc[k, 1]) != str:
                            to_drop.append(k)
                        elif df_temp2.iloc[k, 1] == '':
                            to_drop.append(k)
                        elif df_temp2.iloc[k, 1][0] != 'R':
                            to_drop.append(k)
                    df_temp2 = df_temp2.drop(to_drop).reset_index(drop=True)
                    to_drop = []
                    for k in range(len(df_temp3)):
                        if type(df_temp3.iloc[k, 1]) != str:
                            to_drop.append(k)
                        elif df_temp3.iloc[k, 1] == '':
                            to_drop.append(k)
                        elif df_temp3.iloc[k, 1][0] != 'R':
                            to_drop.append(k)
                    df_temp3 = df_temp3.drop(to_drop).reset_index(drop=True)
                    df.columns = ['Nazwa', 'Kod', 'C0010', 'C0020', 'C0030', 'C0040', 'C0050', 'C0060', 'C0070',
                                       'C0080', 'C0090']
                    df_temp.columns = ['Nazwa', 'Kod', 'C0100', 'C0110', 'C0120', 'C0130', 'C0140', 'C0150',
                                       'C0160', 'C0200']
                    df_temp2.columns = ['Nazwa', 'Kod', 'C0010', 'C0020', 'C0030', 'C0040', 'C0050', 'C0060', 'C0070',
                                  'C0080', 'C0090']
                    df_temp3.columns = ['Nazwa', 'Kod', 'C0100', 'C0110', 'C0120', 'C0130', 'C0140', 'C0150',
                                       'C0160', 'C0200']
                    df = df.join(df_temp, rsuffix='_temp').drop(['Nazwa_temp', 'Kod_temp'], axis=1)
                    df_temp2 = df_temp2.join(df_temp3, rsuffix='_temp').drop(['Nazwa_temp', 'Kod_temp'], axis=1)
                    df = pd.concat([df, df_temp2]).reset_index(drop=True)
                elif df.where(df == 'R0110').notnull().sum().sum() > 0 and df.where(df == 'R0300').notnull().sum().sum() == 0:
                    df_temp = tables[i+1]
                    df_temp2 = tables[i + 2]
                    df_temp3 = tables[i + 4]
                    df_temp4 = tables[i + 5]
                    df_temp5 = tables[i + 6]
                    groups[i] = ''
                    groups[i + 1] = ''
                    groups[i + 2] = ''
                    groups[i + 4] = ''
                    groups[i + 5] = ''
                    groups[i + 6] = ''
                    to_drop = []
                    for k in range(len(df)):
                        if type(df.iloc[k, 1]) != str:
                            to_drop.append(k)
                        elif df.iloc[k, 1] == '':
                            to_drop.append(k)
                        elif df.iloc[k, 1][0] != 'R':
                            to_drop.append(k)
                    df = df.drop(to_drop).reset_index(drop=True)
                    to_drop = []
                    df.columns = ['Nazwa', 'Kod', 'C0010', 'C0020', 'C0030', 'C0040', 'C0050', 'C0060', 'C0070',
                                  'C0080', 'C0090']
                    for k in range(len(df_temp)):
                        if type(df_temp.iloc[k, 1]) != str:
                            to_drop.append(k)
                        elif df_temp.iloc[k, 1] == '':
                            to_drop.append(k)
                        elif df_temp.iloc[k, 1][0] != 'R':
                            to_drop.append(k)
                    df_temp = df_temp.drop(to_drop).reset_index(drop=True)
                    df_temp.columns = ['Nazwa', 'Kod', 'C0010', 'C0020', 'C0030', 'C0040', 'C0050', 'C0060', 'C0070',
                                  'C0080', 'C0090']
                    to_drop = []
                    for k in range(len(df_temp2)):
                        if type(df_temp2.iloc[k, 1]) != str:
                            to_drop.append(k)
                        elif df_temp2.iloc[k, 1] == '':
                            to_drop.append(k)
                        elif df_temp2.iloc[k, 1][0] != 'R':
                            to_drop.append(k)
                    df_temp2 = df_temp2.drop(to_drop).reset_index(drop=True)
                    df_temp2.columns = ['Nazwa', 'Kod', 'C0010', 'C0020', 'C0030', 'C0040', 'C0050', 'C0060', 'C0070',
                                       'C0080', 'C0090']
                    df_temp3.columns = ['C0100', 'C0110', 'C0120', 'C0130', 'C0140', 'C0150',
                                       'C0160', 'C0200']
                    df_temp4.columns = ['C0100', 'C0110', 'C0120', 'C0130', 'C0140', 'C0150',
                                       'C0160', 'C0200']
                    df_temp5.columns = ['C0100', 'C0110', 'C0120', 'C0130', 'C0140', 'C0150',
                                       'C0160', 'C0200']
                    df = pd.concat([df, df_temp3], axis=1)
                    df_temp = pd.concat([df_temp, df_temp4], axis=1)
                    df_temp2 = pd.concat([df_temp2, df_temp5], axis=1)
                    df = pd.concat([df, df_temp, df_temp2]).reset_index(drop=True)
        doc.close()
