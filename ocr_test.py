import pandas as pd
from img2table.ocr import SuryaOCR
from img2table.document import Image
import pymupdf
import os
import re
import numpy as np

from Extractor import new_row

# te ustawienia, co wcześniej do czytania tabel, później się je wywali
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

# wczytanie modułu do czytania tabel
# ocr = SuryaOCR(langs=['pl'])
# for file in os.listdir(os.getcwd()):
#     if file[len(file)-3:len(file)] != 'pdf':
#         continue
#     print("File: " + file)
#     doc = pymupdf.open(file)
#     first = 0
#     # sprawdzenie od której strony zaczynają się tabelki
#     for i in range(1, 5):
#         page = doc.load_page(i)
#         text = str(page.get_text().encode("utf8"))
#         match = re.search(r'(\d+)(?!.*\d)', text)
#         if match:
#             # któraś z firm wrzucała raporty po 2 strony rzeczywiste na stronę pdf, więc przeczytaną stronę trzeba na dwa podzielić
#             if len(doc) > 60:
#                 if first < int(match.group(1)) < len(doc):
#                     first = int(match.group(1))
#             elif first < int(match.group(1))/2 < len(doc):
#                 first = int(np.floor(int(match.group(1)) / 2))
#     # kod tabeli (jeśli na kolejnej stronie go nie ma, to zakładam, że jest taki sam)
#     match_prev = None
#     firma = file[:len(file) - 4].split('_')[0]
#     rok = file[:len(file) - 4].split('_')[1]
#     # licznik przydatny jeśli tabela o danym kodzie jest na więcej niż jednej stronie (żeby się nie nadpisywały pliki)
#     counter = 1
#     for i in range(first, len(doc)):
#         page = doc.load_page(i)
#         # jeśli (tak jak w Allianzie z 2022) są dane też z innego roku, to ich nie chcemy
#         if re.search(rf'31-12-{int(rok)-1}', str(page.get_text().encode("utf8"))):
#             break
#         match = re.search(r'S\.\d{2}\.\d{2}\.\d{2}', str(page.get_text().encode("utf8")))
#         if match:
#             if match_prev is not None:
#                 if match.group(0) != match_prev.group(0):
#                     counter = 1
#             match_prev = match
#         elif match_prev is not None:
#             counter = 1
#             match = match_prev
#         if match:
#             print(f'Pattern on page {i} identified.')
#             tabela = match.group(0).replace('.','')
#             dirpath = os.path.join(os.getcwd(), "DANE")
#             if not os.path.exists(dirpath):
#                 os.makedirs(dirpath)
#             path = os.path.join(dirpath, f"{firma}_{rok}_{tabela}_{counter}.xlsx")
#             if os.path.exists(path):
#                 os.remove(path)
#             """
#             Pętla poniżej służy do ekstrakcji tabel ze strony, tylko że sama funkcja na wejściu nie przyjmuje pdf, tylko
#             obrazek, więc najpierw konwertuje stronę na obrazek a potem go czytam. Jest to wszystko w pętli, przez pierwszy
#             jej wiersz, chodzi o to, że jak te argumenty są za małe lub zbyt duże, to słabo czyta, więc przechodzi przez
#             kilka opcji i jak coś znajdzie i wyciągnie, to przerywa pętle.
#             """
#             for j in range(4):
#                 pix = page.get_pixmap(matrix=pymupdf.Matrix(float(4-j), float(4-j)))
#                 pix.save('temp.jpg')
#                 img = Image('temp.jpg')
#                 """
#                 z tej biblioteki można tylko do excela zapisać, jak się po prostu wyciąga, bez zapisu, to spory bałagan
#                 jest, a tak to te wyciągnięte tabele też są zapisane i nie trzeba tego w kółko robić
#                 """
#                 img.to_xlsx(dest=path,
#                         ocr=ocr,
#                             implicit_rows=True,
#                             implicit_columns=True,
#                             borderless_tables=True,
#                             min_confidence=1)
#                 temp = pd.read_excel(path)
#                 if not temp.empty:
#                     break
#             print(f'Data from page {i} loaded.')
#             counter += 1
#     print(f"File {file} completed.")
#     doc.close()

""" JEŚLI CHCESZ CZYTAĆ DANE Z EXCELI TO ZAKOMENTUJ KOD POWYŻEJ, BO SPORO ŻYCIA NA TYM STRACISZ XD """
# listy do trzymania danych
table_list = []
table_year = []
table_company = []
table_names = []
path = os.path.join(os.getcwd(), 'DANE')
for file in os.listdir(path):
    company = file.split('_')[0]
    year = file.split('_')[1]
    table_name = file.split('_')[2]
    # if not (company == 'Warta' and year == '2017'):
    #     continue
    """Jak zapisuje wcześniej do excela, to często jest wiele arkuszy w jednym skoroszycie, więc trzeba przez wszystkie
    przejśc"""
    tables = pd.ExcelFile(os.path.join(os.getcwd(), 'DANE', file))
    for sheet in tables.sheet_names:
        table = pd.read_excel(os.path.join(path, file), sheet, header=None)
        """Jeśli skoroszyt jest pusty, to nie idziemy dalej"""
        if tables.sheet_names.index(sheet) == 0 and table.empty:
            # print('No data found in table: ' + str(file.split('.')[0]))
            break
        table = table.astype(str).replace(['nan', 'o'], [pd.NA, '0']).replace(np.nan, pd.NA)
        """To się później przydaje do czyszczenia"""
        x_prev = len(table)
        y_prev = len(table.columns)
        """Tutaj pierwsze ogólne czyszczenie, ale tylko od góry i od lewej, docelowo w pierwszej kolumnie mają być kody R
         a w pierwszym wierszu kody C, jest przerywane, jeśli po jednej iteracji nic nie wyrzucono
        """
        while True:
            j = 0
            stop = False
            while j < len(table.columns):
                for i in range(len(table)):
                    if not pd.isna(table.iloc[i, j]):
                        if table.iloc[i, j].replace(' ', '').replace('.', '').replace(',','').replace('%', '').isdigit() or (re.search(r'(R\d{4})|(C\d{4})', table.iloc[i, j]) and not re.search(r'\n', table.iloc[i, j])):
                            stop = True
                            break
                if stop:
                    table = table.iloc[:, j:]
                    break
                j += 1
            to_drop = []
            i = 0
            c = None
            stop = -1
            while i < len(table):
                if re.search(r'C\d{4}',np.array2string(table.iloc[i, :].values)):
                    if re.findall(r'C\d{4}',np.array2string(table.iloc[i, :].values)) == c:
                        to_drop.append(i)
                    else:
                        c = re.findall(r'C\d{4}', np.array2string(table.iloc[i, :].values))
                if stop == -1:
                    for j in range(len(table.columns)):
                        if not pd.isna(table.iloc[i, j]):
                            if table.iloc[i, j].replace(' ', '').replace('.', '').replace(',','').replace('-','').isdigit() or re.search(r'(R\d{4})|(C\d{4})', table.iloc[i, j]):
                                stop = i
                                break
                i += 1
            if len(to_drop) > 0:
                table = table.drop(to_drop).reset_index(drop=True)
            if stop != -1:
                table = table.iloc[stop:, :].reset_index(drop=True)
            x = len(table)
            y = len(table.columns)
            if x == x_prev and y == y_prev:
                break
            else:
                x_prev = x
                y_prev = y
        """Puste kolumny póki co zostają (na wszelki wypadek)"""
        table = table.dropna(how='all').reset_index(drop=True)
        """Jeśli brakuje kodów C, to dorzucam je z poprzedniej tabeli (jeśli ma ten sam kod S)"""
        if len(re.findall(r'C\d{4}',np.array2string(table.iloc[:, :].values))) == 0:
            if len(table_list) > 0 and table_name != 'S020102':
                if table_names[-1] == table_name:
                    temp_data = re.findall(r'C\d{4}',np.array2string(table_list[-1].iloc[:, :].values))
                    while len(temp_data) < len(table.columns):
                        temp_data.insert(0, pd.NA)
                    temp_data = pd.DataFrame([temp_data])
                    for i in range(len(table.columns)):
                        table = table.rename(columns={table.columns[i]: i})
                    for i in range(len(temp_data.columns)):
                        temp_data = temp_data.rename(columns={temp_data.columns[i]: i})
                    table = pd.concat([temp_data, table], ignore_index=True)
            elif table_name == 'S020102':
                new_row = [pd.NA]*(len(table.columns) - 1) + ['C0010']
                table = pd.concat([pd.DataFrame([new_row], columns=table.columns), table], ignore_index=True)

        """ Tutaj już tak definitywnie docinam od lewej"""
        for i in range(len(table.columns)):
            if re.search(r'(C\d{4})|(R\d{4})', np.array2string(table.iloc[:, i].values)):
                table = table.iloc[:, i:]
                break
        table = table.dropna(how='all')
        """Czasami jest tak, że kod C jest w jednej kolumnie, a wszystkie wartości w drugiej, więc tutaj temu przeciwdziałam
        (podmieniam pierwsze wartości kolumn)"""
        for i in range(len(table.columns) - 1):
            if not pd.isna(table.iloc[0, i]):
                if re.search(r'C\d{4}', table.iloc[0, i]) and table.iloc[1:len(table), i].isna().sum() == len(table) - 1 and pd.isna(table.iloc[0, i+1]):
                    table.iloc[0, i+1] = table.iloc[0, i]
                    table.iloc[0, i] = pd.NA
        table = table.dropna(how='all').reset_index(drop=True).dropna(axis=1, how='all')
        """
        Osobno obsługuję tabele S190121 (ta z N-1, N-2 itd.), bo tam często jest wiele tabel na jednej stronie, więc łatwiej
        je pociąć na mniejsze i osobno zrobić
        """
        if table_name != 'S190121':
            table_list.append(table)
            table_names.append(table_name)
            table_company.append(company)
            table_year.append(year)
        else:
            R = False
            for i in range(len(table.columns)):
                if re.search(r'R\d{4}', np.array2string(table.iloc[:, i].values)):
                    if not R:
                        R = True
                    else:
                        table_list.append(table.iloc[:, :i].dropna(how='all').reset_index(drop=True).dropna(axis=1, how='all'))
                        table_names.append(table_name)
                        table_list.append(table.iloc[:, i:].dropna(how='all').reset_index(drop=True).dropna(axis=1, how='all'))
                        table_names.append(table_name)
                        table_company.append(company)
                        table_year.append(year)
                        table_company.append(company)
                        table_year.append(year)

fin_table = []
num_tab = 0
num_dat = 0
for i in range(len(table_list)):
    print(table_names[i])
    print(table_list[i])
    print(200*'*')
    num_dat += (len(table_list[i]) - 1) * (len(table_list[i].columns) - 1)
    """ Tutaj jest cała masa sprawdzania i finalnego zapisywania danych, tego szczegółowo nie opisuję, bo to na pewno
    zredukuję w przyszłości xd, S120102 nie czytam, bo jakoś słabo ją czytało, a też nic takiego ważnego nie ma"""
    if table_names[i] != 'S120102' and re.search(r'C\d{4}', np.array2string(table_list[i].iloc[0, :].values)) and re.search(r'R\d{4}', np.array2string(table_list[i].iloc[:, 0].values)):
        num_tab += 1
        for j in range(1, len(table_list[i])):
            for k in range(1, len(table_list[i].columns)):
                if not (pd.isna(table_year[i]) or pd.isna(table_names[i]) or pd.isna(table_list[i].iloc[0, k]) or pd.isna(table_list[i].iloc[j, 0]) or pd.isna(table_list[i].iloc[j, k])):
                    if re.search(r'C\d{4}', table_list[i].iloc[0, k]) and re.search(r'R\d{4}', table_list[i].iloc[j, 0]) and not pd.isna(table_list[i].iloc[j, k]):
                        if len(table_list[i].iloc[j, k]) == 1:
                            table_list[i].iloc[j, k] = '0'
                        if table_list[i].iloc[j, k].replace(' ', '').replace('.', '').replace(',','').replace('-','').replace('%', '').isdigit():
                            if re.search(r'C\d{4}', table_list[i].replace(pd.NA, '').iloc[0, 0]):
                                fin_table.append([table_company[i], table_year[i], table_names[i], table_list[i].iloc[0, k-1], table_list[i].iloc[j, 0], table_list[i].iloc[j, k]])
                            else:
                                fin_table.append(
                                    [table_company[i], table_year[i], table_names[i], table_list[i].iloc[0, k],
                                     table_list[i].iloc[j, 0], table_list[i].iloc[j, k]])
    elif table_names[i] == 'S280101' and re.search(r'C0010', np.array2string(table_list[i].iloc[:, :].values)):
        """
            Te czytam osobno, bo mają tylko jedną wartość, a często zjada kod R
            """
        num_tab += 1
        for j in range(len(table_list[i])):
            for k in range(len(table_list[i].columns)):
                if not pd.isna(table_list[i].iloc[j, k]):
                    if table_list[i].iloc[j, k].replace(' ', '').isdigit():
                        fin_table.append([table_company[i], table_year[i], 'S280101', 'C0010', 'R0010', table_list[i].iloc[j, k]])
    elif table_names[i] == 'S280101' and re.search(r'C0040', str(table_list[i])):
        num_tab += 1
        for j in range(len(table_list[i])):
            for k in range(len(table_list[i].columns)):
                if not pd.isna(table_list[i].iloc[j, k]):
                    if table_list[i].iloc[j, k].replace(' ', '').isdigit():
                        fin_table.append([table_company[i], table_year[i], 'S280101', 'C0040', 'R0200', table_list[i].iloc[j, k]])

for i in range(len(fin_table)):
    fin_table[i][5] = str(fin_table[i][5]).replace(' ', '').replace(',', '.').replace('–', '-')
    if fin_table[i][5].find('%') != -1:
        fin_table[i][5] = str(float(fin_table[i][5].replace('%', '')) / 100)
    else:
        fin_table[i][5] = str(float(fin_table[i][5]) * 1000)
    if fin_table[i][5].find('.') == -1:
        fin_table[i][5] += '.0'

print(f'Number of tables extracted: {len(table_list)}')
print(f'Number of tables read: {num_tab}')
print(f'Number of cells extracted: {num_dat}')
print(f'Number of cells read: {len(fin_table)}')
table = pd.DataFrame(fin_table)
table.columns = ['Firma', 'Rok', 'Kod_S', 'Kod_C', 'Kod_R', 'Wartosc']
table.to_csv('all.csv', index=False, mode='w+')
# print(table.groupby(['Firma', 'Rok'])['Kod_S'].count())
# print(table.groupby('Firma')['Kod_S'].count())
# print(table.groupby('Rok')['Kod_S'].count())