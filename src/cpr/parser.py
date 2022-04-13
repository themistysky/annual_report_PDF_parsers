"""Parse PDF files of CPR."""
import os.path
import tabula as tb
import pandas as pd
import numpy as np
import re
import sys
from os.path import exists
import PyPDF2
import requests
import glob

with open(r"./src/cpr/currencies.txt") as f:
    currencies = f.readline().split()


def findPages(file,name):
    object = PyPDF2.PdfFileReader(file)
    pages = []
    # get number of pages
    NumPages = object.getNumPages()

    name = name.lower().split('-')[1].replace(' ','')
    String = name + 'quantitdnominationdevisedecotation%actifsnetsvaleurd\'valuation'
    String2 = name + '(lancle15/10/20)'+'quantitdnominationdevisedecotation%actifsnetsvaleurd\'valuation'
    String3 = name + '(lancle25/06/20)'+'quantitdnominationdevisedecotation%actifsnetsvaleurd\'valuation'
    s3 = [name , '(lancle25/06/20)','actifsnets']

    # extract text and do the search
    check = False
    for i in range(0, NumPages):
        PageObj = object.getPage(i)
        Text = PageObj.extractText()
        Text = Text.replace('\n',' ').replace(' ','').lower()

        if (String in Text) or (String2 in Text) or (String3 in Text) or (all(x in Text for x in s3)):
            pages.append(i+1)
            check = True
        elif check==True:
            break

    return pages

def findPages2(file,name):
    object = PyPDF2.PdfFileReader(file)
    pages = []

    # get number of pages
    NumPages = object.getNumPages()

    # define keyterms
    #String = "3.12."
    name = name.split('-')[0].lower().replace(' ','')
    if name.startswith('cpr'):
        name = name[3:-6]

    Strings = ['3.12.inventairedétaillédesinstrumentsfinancierseneurdésignationdesvaleursdeviseqténbreounominalvaleuractuelle%actifnet',name]
    Strings2 = ['inventairedtailldesinstrumentsfinancierseneur',name]
    
    # extract text and do the search
    check = False
    for i in range(0, NumPages):
        PageObj = object.getPage(i)
        Text = PageObj.extractText()
        Text = Text.replace('\n',' ').replace(' ','').lower()
#        ResSearch = re.search(String, Text, re.IGNORECASE)
#        if ResSearch != None:
        if all(x in Text for x in Strings) or all(x in Text for x in Strings2):
            pages.append(i+1)
#             check = True
#         elif check==True:
#             break

    return pages

def download_file(download_url, filename):
    """Download the file from url.

    Keyword Arguments:
    download_url -- pdf link
    filename -- PDF file name
    """
    response = requests.get(download_url)
    file = open(filename, "wb")
    file.write(response.content)
    file.close()


def main():
    """Main."""
    assert "-i" in sys.argv, "No input file path provided as argument."
    assert "-o" in sys.argv, "No output file path provided as argument."
    assert "-p" in sys.argv, "No download path provided as argument."
    in_path = sys.argv[sys.argv.index("-i") + 1]
    out_path = sys.argv[sys.argv.index("-o") + 1]
    pdf_out_path = sys.argv[sys.argv.index("-p") + 1]

    meta_data = pd.read_csv(in_path)
    final_data = []
    for i in range(len(meta_data)):
        url = meta_data.iloc[i, 1]
        file = os.path.join(
            meta_data.iloc[i, 0] + ".pdf"
        )  # Can give full path here
        fund_name = meta_data.iloc[i, 0]
        if os.path.isfile(file) == 0:
            print("downloading ", url)
            download_file(url, file)
        else:
            print("downloaded")
        pages = find_start_page(file, fund_name)
        print(file, pages)
        file_data = []

        for j in pages:
            df = tb.read_pdf(
                file,
                pages=j,
                area=(0, 0, 850, 950),
                columns=[320, 360, 420, 512],
                pandas_options={"header": None},
                stream=True,
            )
            file_data.append(df[0])
        df = pd.concat(file_data, ignore_index=True)
        # df = df[df[3].apply(lambda x: False if x.endswith(",") else True)]
        df = df.drop(2, axis=1)
        df = df.rename(
            columns={
                0: "holding_name",
                1: "currency",
                3: "market_value",
                4: "net_assets",
            }
        )

        to_keep = ["Créances", "Dettes", "Comptes financiers"]
        df["currency"] = df["currency"].apply(lambda x: str(x).strip())
        df = df[
            (
                df["currency"].isin(currencies)
                | df["holding_name"].isin(to_keep)
            )
            & df["net_assets"].notna()
        ].reset_index(drop=True)
        df["market_value"] = df["market_value"].apply(
            lambda x: float(str(x).replace(",", ".").replace(" ", "").strip())
        )
        df["net_assets"] = df["net_assets"].apply(
            lambda x: float(str(x).replace(",", ".").replace(" ", "").strip())
        )
        df["holding_name"] = df["holding_name"].str.strip()

        df["fund_name_website"] = df["fund_name_report"] = meta_data.iloc[i, 0]
        df["pdf_url"] = meta_data["pdf_url"][i]
        mode = df.currency.mode()[0]
        for j, r in df[1:].iterrows():
            if r["currency"] == "nan":
                df["currency"][j] = mode
            if str(r["holding_name"]) == "nan":
                df["holding_name"][j] = df["holding_name"][j - 1]
        if "isin" in meta_data.columns:
            df["isin"] = meta_data["isin"][i]
        final_data.append(df)
        print("net_assets_sum -", df["net_assets"].sum())
        print("market_value_sum -", df["market_value"].sum())
        print(len(final_data[-1]))

    final_data = pd.concat(final_data).reset_index(drop=True)
    final_data["fund_provider"] = "Amundi Asset Management (FR)"
    cols = [
        "fund_provider",
        "fund_name_report",
        "fund_name_website",
        "isin",
        "holding_name",
        "currency",
        "market_value",
        "net_assets",
        "pdf_url",
    ]
    print(
        final_data[
            ["holding_name", "currency", "market_value", "net_assets"]
        ].count()
    )
    final_data = final_data[cols]
    print("Total rows: ", len(final_data))
    final_data.to_csv(out_path, index=False)


if __name__ == "__main__":
    main()
