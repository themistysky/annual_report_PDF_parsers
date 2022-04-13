"""Takes CSV as input. Downloads the PDF files and.

Parses PDFs of provider La banque postale AM. Generates a CSV file
containing parsed results.
"""
import re
import sys
import os
import warnings
from pdfminer.high_level import extract_text
from PyPDF2 import PdfFileReader
import tabula
import pandas as pd
import numpy as np
import requests

warnings.filterwarnings("ignore")
pd.set_option("display.max_rows", 18000)
pd.set_option("display.max_columns", 2000)


f = open("./src/banque/currencies.txt")
currencies = f.readline().split()
f.close()
currencies.extend(["CNH", "CHN"])
done1 = {}


def parse(pdf, read_file, website, pdf_url):
    """Parse all types of PDF of PDF of banque.

    Keyword Arguments:
    pdf -- PdfFileReader() object
    read_file -- file name
    website -- fund_name_website
    pdf_url -- pdf link

    Return Pandas DataFrame.
    """
    typ = 0
    if read_file in done1:
        table = done1[read_file]
        print(" memory")
    else:
        pages = []
        for i in range(pdf.getNumPages() - 3, 0, -1):
            text = extract_text(read_file, page_numbers=[i]).replace("\n", "")
            if re.search(
                r"Désignation.*des.*valeurs.*Devise.*Qté.*Nbreou", text
            ):
                typ = 1
                k = i
                while bool(
                    re.search(
                        r"Désignation.*des.*valeurs.*Devise.*Qté.*Nbreou", text
                    )
                ):
                    pages.append(k + 1)
                    k -= 1
                    text = extract_text(read_file, page_numbers=[k]).replace(
                        "\n", ""
                    )
                break
            if re.search(
                r"Désignation.*des.*valeurs.*Quantité.*Cours.*Devise", text
            ):
                typ = 2
                k = i
                while bool(
                    re.search(
                        r"Désignation.*des.*valeurs.*Quantité.*Cours.*Devise",
                        text,
                    )
                ):
                    pages.append(k + 1)
                    k -= 1
                    text = extract_text(read_file, page_numbers=[k]).replace(
                        "\n", ""
                    )
                break
        pages = pages[::-1]
        print(pages)
        if typ == 1:
            tables = tabula.read_pdf(
                read_file,
                pages=pages,
                area=[0, 0, 828, 590],
                columns=[339, 366, 436, 514],
                silent=True,
                guess=False,
                stream=True,
            )
            for i in range(len(tables)):
                tables[i].columns = [
                    "holding_name",
                    "currency",
                    "_1",
                    "market_value",
                    "net_assets",
                ]
                tables[i].drop(columns=["_1"], inplace=True)
        elif typ == 2:
            tables = tabula.read_pdf(
                read_file,
                pages=pages,
                area=[0, 0, 828, 590],
                columns=[308, 369, 421, 448, 516, 560],
                silent=True,
                guess=False,
                stream=True,
            )
            for i in range(len(tables)):
                tables[i].columns = [
                    "holding_name",
                    "_1",
                    "_2",
                    "currency",
                    "market_value",
                    "net_assets",
                ]
                tables[i].drop(columns=["_1", "_2"], inplace=True)
        else:
            return []
        table = pd.concat(tables, ignore_index=True)
        to_keep = ["Dettes", "Comptes financiers", "Créances"]
        table = table[
            (
                table["currency"].isin(currencies)
                | table["holding_name"].isin(to_keep)
            )
            & ~(table["holding_name"] == "TOTAL ACTIF NET")
        ]
        table = table[~table["net_assets"].isna()]
        table["market_value"] = table["market_value"].apply(
            lambda x: "0.0" if str(x) in ["-", "nan", "–"] else x
        )
        table["net_assets"] = table["net_assets"].apply(
            lambda x: "0.0" if str(x) in ["-", "nan", "–"] else x
        )
        table["market_value"] = (
            table["market_value"]
            .str.replace(",", ".")
            .replace("\\s", "", regex=True)
            .replace("[^-.0-9]", "", regex=True)
            .astype(float)
        )
        table["net_assets"] = (
            table["net_assets"]
            .str.replace(",", ".")
            .replace("\\s", "", regex=True)
            .replace("[^-.0-9]", "", regex=True)
            .astype(float)
        )
        done1[read_file] = table
    table["fund_name_website"] = website
    table["fund_name_report"] = (
        " ".join(website.split()[:-1])
        if len(website.split()[-1]) < 4
        else website
    )
    table["pdf_url"] = pdf_url
    return table.reset_index(drop=True)


def main():
    """Read input CSV and call parser function."""
    assert "-i" in sys.argv, "No input file path provided as argument."
    assert "-o" in sys.argv, "No output file path provided as argument."
    assert "-p" in sys.argv, "No download path provided as argument."
    in_path = sys.argv[sys.argv.index("-i") + 1]
    out_path = sys.argv[sys.argv.index("-o") + 1]
    pdf_out_path = sys.argv[sys.argv.index("-p") + 1]
    currdir = os.getcwd()
    print("Taking input from " + in_path)

    pdf_links_df = pd.read_csv(os.path.join(currdir, in_path))
    links_set = set(pdf_links_df["pdf_url"])
    links_set.discard("annual_report_does_not_exists")
    os.chdir(os.path.join(currdir, os.path.normpath(pdf_out_path)))

    if len(pdf_links_df.columns) == 2:
        links_set = set(pdf_links_df["pdf_url"])
        links_set.discard("nan")
        links_set.discard("annual_report_does_not_exists")
        links_to_pdf_map = {}
        num = 1
        print("Downloading " + str(len(links_set)) + " files...")
        for link in links_set:
            if link in ["nan", "annual_report_does_not_exists"]:
                continue
            response = requests.get(link)
            links_to_pdf_map[link] = "banque" + str(num) + ".pdf"
            with open("banque" + str(num) + ".pdf", "wb") as f:
                print("banque" + str(num) + ".pdf")
                f.write(response.content)
            num += 1
        del num
        links_to_pdf_map[np.nan] = np.nan
        links_to_pdf_map["annual_report_does_not_exists"] = np.nan
        pdf_links_df["files"] = pdf_links_df["pdf_url"].apply(
            lambda x: links_to_pdf_map[x]
        )
        pdf_links_df.to_csv("pdf_names.csv", mode="w", index=False)
    tables = []
    done = {}
    for idx, row in pdf_links_df.iterrows():
        website = row["name"]
        read_file = row["files"]
        if str(read_file) == "nan":
            continue
        pdf_url = row["pdf_url"]
        if read_file in done:
            pdf = done[read_file]
        else:
            pdf = PdfFileReader(read_file)
            done[read_file] = pdf
        pdf._override_encryption = True
        pdf._flatten()
        print("parsing ", read_file, website, end="  ")
        table = parse(pdf, read_file, website, pdf_url)
        if len(table) > 0:
            tables.append(table)
    result = pd.concat(tables, ignore_index=True)
    result["currency"] = result["currency"].apply(lambda x: str(x).strip())
    result["holding_name"] = result["holding_name"].apply(
        lambda x: str(x).strip()
    )
    result = result[~result["net_assets"].isna()]
    result = result.reset_index(drop=True)
    for i, r in result.iterrows():
        if str(r["currency"]) == "nan":
            result["currency"][i] = result["currency"][i - 1]
    result["holding_name"] = result["holding_name"].apply(
        lambda x: "_" if x == "nan" else x
    )
    result["fund_provider"] = "La banque postale AM"
    result["isin"] = None
    result = result[
        [
            "fund_provider",
            "fund_name_report",
            "fund_name_website",
            "isin",
            "holding_name",
            "market_value",
            "currency",
            "net_assets",
            "pdf_url",
        ]
    ]

    print("Total - ", len(result))
    result.to_csv(
        os.path.join(currdir, os.path.normpath(out_path)),
        index=False,
        encoding="utf-8",
        mode="w",
    )


if __name__ == "__main__":
    main()
