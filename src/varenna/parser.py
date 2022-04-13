"""Takes CSV as input. Downloads the PDF files and.

Parses PDFs of provider Varenna. Generates a CSV file
containing parsed results.
"""
import re
import sys
import os
import warnings
import tabula
from PyPDF2 import PdfFileReader
import pandas as pd
import numpy as np
import requests

warnings.filterwarnings("ignore")

with open("./src/varenna/currencies.txt") as f:
    currencies = f.readline().split()


def main():
    """Read input CSV and call parser functions."""
    assert "-i" in sys.argv, "No input file path provided as argument."
    assert "-p" in sys.argv, "No download path provided as argument."
    assert "-o" in sys.argv, "No output file path provided as argument."
    in_path = sys.argv[sys.argv.index("-i") + 1]
    out_path = sys.argv[sys.argv.index("-o") + 1]
    pdf_out_path = sys.argv[sys.argv.index("-p") + 1]
    currdir = os.getcwd()
    print("Taking input from " + in_path)
    pdf_links_df = pd.read_csv(
        os.path.join(currdir, os.path.normpath(in_path))
    )
    links_set = set(pdf_links_df["pdf_url"])
    links_set.discard("annual_report_does_not_exists")
    os.chdir(os.path.join(currdir, os.path.normpath(pdf_out_path)))
    if "files" not in pdf_links_df.columns:
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
            links_to_pdf_map[link] = "varenna" + str(num) + ".pdf"
            with open("varenna" + str(num) + ".pdf", "wb") as f:
                print("varenna" + str(num) + ".pdf")
                for chunk in response.iter_content(chunk_size=1024):
                    if chunk:
                        f.write(chunk)
            num += 1
        del num, response
        links_to_pdf_map[np.nan] = np.nan
        links_to_pdf_map["annual_report_does_not_exists"] = np.nan
        pdf_links_df["files"] = pdf_links_df["pdf_url"].apply(
            lambda x: links_to_pdf_map[x]
        )
        pdf_links_df.to_csv("pdf_names.csv", mode="w", index=False)
    fund_to_tables = {}
    for index, row in pdf_links_df.iterrows():
        read_file = row["files"]
        fund_website = row["name"]
        pdf = PdfFileReader(read_file)
        total_pages = pdf.getNumPages()
        text = "INVENTAIRE DÉTAILLÉ DES INSTRUMENTS FINANCIERS"
        pages = [
            i + 1
            for i in range(total_pages - 1, total_pages - 16, -1)
            if text in pdf.getPage(i).extractText()
        ]
        tables = tabula.read_pdf(
            read_file,
            pages=pages,
            stream=True,
            area=(50, 50, 765, 570),
            columns=(321, 356, 417, 499),
            guess=False,
            silent=True,
            encoding="utf-8",
        )
        fund_to_tables[fund_website] = list(tables) + [row["pdf_url"]]
        print("parsing ", read_file, fund_website, (pages[0], pages[-1]))
    result = pd.DataFrame(
        columns=[
            "fund_name_report",
            "fund_name_website",
            "holding_name",
            "currency",
            "market_value",
            "net_assets",
            "pdf_url",
        ]
    )
    for report in fund_to_tables:
        tables, pdf_url = (
            fund_to_tables[report][:-1],
            fund_to_tables[report][-1],
        )
        for table in tables:
            table.columns = [
                "holding_name",
                "currency",
                "_",
                "market_value",
                "net_assets",
            ]
            table["fund_name_website"] = table["fund_name_report"] = report
            table["pdf_url"] = pdf_url
            result = pd.concat(
                [
                    result,
                    table[
                        [
                            "fund_name_report",
                            "fund_name_website",
                            "holding_name",
                            "currency",
                            "market_value",
                            "net_assets",
                            "pdf_url",
                        ]
                    ],
                ],
                ignore_index=True,
            )
    to_keep = ["Créances", "Dettes", "Comptes financiers"]
    result = result[
        (
            result["currency"].isin(currencies)
            | result["holding_name"].isin(to_keep)
        )
        & ~(
            (result["net_assets"].apply(lambda x: str(x).isdigit()))
            | (result["net_assets"].isna())
        )
    ]
    result["market_value"] = result["market_value"].apply(
        lambda x: float(str(x).replace(" ", "").replace(",", "."))
    )
    result["net_assets"] = result["net_assets"].apply(
        lambda x: float(str(x).replace(" ", "").replace(",", "."))
    )
    result["holding_name"] = result["holding_name"].apply(lambda x: x.strip())
    result = result.reset_index(drop=True)
    for i, r in result.iterrows():
        if str(r["currency"]) == "nan":
            result["currency"][i] = result["currency"][i - 1]
    result.insert(0, "fund_provider", value="Varenne capital")
    result.insert(3, "isin", value=None)
    print("Total - ", len(result))
    result.to_csv(
        os.path.join(currdir, os.path.normpath(out_path)),
        index=False,
        encoding="utf-8",
        mode="w",
    )


if __name__ == "__main__":
    main()
