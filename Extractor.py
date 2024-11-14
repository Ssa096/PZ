import camelot
import os
import pymupdf
import re
import sys

def extract():
    for file in os.listdir(os.getcwd()):
        if file[len(file)-3:len(file)] == 'pdf':
            doc = pymupdf.open(file)
            first = 0
            for i in range(1, 5):
                page = doc.load_page(i)
                text = str(page.get_text().encode("utf8"))
                match = re.search(r'(\d+)(?!.*\d)', text)
                if match:
                    if first < int(match.group(1)) < len(doc):
                        first = int(match.group(1))
            doc.close()
            tables = camelot.read_pdf(file, pages=f'{first}-end', suppress_stdout=True, line_scale=54)
            print(f"Total tables extracted: {len(tables)}")
            for i, table in enumerate(tables):
                df = table.df
                print(table.parsing_report)
                # if df.where(df == "").isnull().sum().sum() != 0:
                dirpath = os.path.join(os.getcwd(), f"{file[0:len(file)-4]}")
                if not os.path.exists(dirpath):
                    os.makedirs(dirpath)
                path = os.path.join(dirpath, f"{file[0:len(file)-4]}_{i}.csv")
                df.to_csv(path, index=False)
extract()