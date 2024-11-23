import os
import pandas as pd

path = os.path.join(os.getcwd(), 'Done2')
for file in os.listdir(path):
    table = pd.read_csv(os.path.join(path, file))
    fin_table = []
    firma = file.split('_')[0]
    rok = file.split('_')[1]
    tabela = file.split('_')[2].split('.')[0]
    for i in range(len(table)):
        for j in range(3, len(table.columns)):
            fin_table.append([firma, rok, tabela, table.columns[j], table.iloc[i, 2], table.iloc[i, 1], table.iloc[i, j]])
    table = pd.DataFrame(fin_table)
    table.columns = ['Firma', 'Rok', 'Kod_S', 'Kod_C', 'Kod_R', 'Nazwa', 'Wartosc']
    table.to_csv(os.path.join(os.getcwd(), 'DANE_KK', f"{firma}_{rok}_{tabela}.csv"), index=False)