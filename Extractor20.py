import pdfplumber
import pandas as pd

with pdfplumber.open("Allianz_2022.pdf") as pdf:
    dane = []
    # Przejdź przez każdą stronę PDF
    for page in pdf.pages:
        # Wyciągnij tabele z bieżącej strony
        tabela = page.extract_table()
        if tabela:
            dane.extend(tabela)

# Utwórz DataFrame z zebranych danych
df = pd.DataFrame(dane[1:], columns=dane[0])  # Pierwszy wiersz jako nagłówki kolumn

# Zapisz dane do pliku Excel
df.to_excel("Allianz.xlsx", index=False)