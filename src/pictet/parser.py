"""Takes CSV as input. Downloads the PDF files and.

Parses PDFs of provider Pictet. Generates a CSV file
containing parsed results.
"""
import os
import sys
import warnings
from pdfminer.high_level import extract_text
import tabula
import pandas as pd
import numpy as np
import requests

warnings.filterwarnings("ignore")

with open("./src/pictet/currencies.txt") as f:
    currencies = f.readline().split()
currencies.append("UNITÉ")
currencies.append("CNH")


file_contents = {}
done = {}


def parse(read_file, website, isin, pdf_url):
    """Parse PDF of Pictet.

    Keyword Arguments:
    read_file-- file name
    website-- fund_name_website
    isin-- ISIN
    pdf_url-- PDF file link
    """
    if read_file in file_contents:
        contents = file_contents[read_file]
    else:
        cont_pgs = [
            i + 1
            for i in range(16)
            if "Table des matières"
            in extract_text(read_file, page_numbers=[i]).replace("\n", "")
        ]
        contents = tabula.read_pdf(
            read_file,
            pages=cont_pgs,
            area=[0, 0, 828, 590],
            columns=[435],
            silent=True,
            guess=False,
        )
        for i in range(len(contents)):
            contents[i].columns = ["fund", "pg"]
        contents = pd.concat(contents, ignore_index=True)
        file_contents[read_file] = contents
    start, end = 0, 0
    for i, r in contents.iterrows():
        if website.split("-")[1].strip() in r["fund"]:
            start, end = (
                int(contents.iloc[i + 1, 1]) + 2,
                int(contents.iloc[i + 2, 1]) + 1,
            )
            break
    if start == end == 0:
        print()
        return []
    pages = tuple(range(start, end + 1))
    print(pages)
    if (read_file, pages) in done:
        table = done[(read_file, pages)]
    else:
        tables = tabula.read_pdf(
            read_file,
            pages=pages,
            area=[0, 0, 828, 590],
            columns=[280, 303, 414, 505],
            silent=True,
            guess=False,
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
        table = pd.concat(tables, ignore_index=True)
        table["holding_name"] = table["holding_name"].apply(
            lambda x: str(x).strip()
        )
        to_keep = [
            "Avoirs en banque",
            "Dépôts bancaires",
            "Autres actifs nets",
        ]
        table = table[
            ~table["net_assets"].isna()
            & (
                table["currency"].isin(currencies)
                | table["holding_name"].isin(to_keep)
            )
        ]
        table["market_value"] = table["market_value"].apply(
            lambda x: float(x.replace(",", ""))
        )
        table["net_assets"] = table["net_assets"].apply(lambda x: float(x))
        table["pdf_url"] = pdf_url
        table["fund_name_report"] = website.split("-")[1].strip()
        table.reset_index(drop=True, inplace=True)
        done[(read_file, pages)] = table
    table["fund_name_website"] = website
    table["isin"] = isin
    return table.reset_index(drop=True)


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
    if len(pdf_links_df.columns) == 3:
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
            links_to_pdf_map[link] = "pictet" + str(num) + ".pdf"
            with open("pictet" + str(num) + ".pdf", "wb") as f:
                print("pictet" + str(num) + ".pdf")
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
    not_found = []
    for idx, row in pdf_links_df.iterrows():
        website = row["name"]
        pdf_url = row["pdf_url"]
        isin = row["isin"]
        read_file = row["files"]
        print("parsing", read_file, " ", website, end=" ")
        table = parse(read_file, website, isin, pdf_url)
        if len(table) == 0:
            not_found.append((read_file, website))
        else:
            tables.append(table)

    result = pd.concat(tables, ignore_index=True)
    for i, r in result.iterrows():
        if str(r["currency"]) == "nan":
            result["currency"][i] = result["currency"][i - 1]
    result["fund_provider"] = "Pictet (FR)"

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
