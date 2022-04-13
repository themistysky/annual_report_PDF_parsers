"""Takes CSV as input. Downloads the PDF files and.

Parses PDFs of provider Scor. Generates a CSV file
containing parsed results.
"""
import re
import sys
import os
import warnings
from PyPDF2 import PdfFileReader
import tabula
import pandas as pd
import numpy as np
import requests

warnings.filterwarnings("ignore")

with open("./src/scor/currencies.txt") as f:
    currencies = f.readline().split()


def parse_type1(pdf, read_file, website, pdf_url):
    """Parse type1 PDF of Scor.

    Keyword Arguments:
    pdf-- PdfFileReader() object
    read_file-- file name
    website-- fund_name_website
    pdf_url-- PDF file link
    """
    pages = [
        i + 1
        for i in range(pdf.getNumPages())
        if "NameQuantity/NominalMarket"
        in pdf.getPage(i).extractText().replace("\n", "")
        and website.upper() in pdf.getPage(i).extractText().replace("\n", "")
    ]
    print(pages)
    tables = tabula.read_pdf(
        read_file,
        pages=pages,
        area=[0, 0, 828, 590],
        columns=[397, 418, 511],
        guess=False,
        silent=True,
    )
    for i in range(len(tables)):
        tables[i].columns = [
            "holding_name",
            "currency",
            "market_value",
            "net_assets",
        ]
    table = pd.concat(tables, ignore_index=True)
    for i, r in table.iterrows():
        if r["currency"] in currencies:
            table["holding_name"][i] = " ".join(r["holding_name"].split()[1:])
    table = table[~table["net_assets"].isna()]
    table["fund_name_report"] = website.upper()
    table["fund_name_website"] = website
    table["pdf_url"] = pdf_url
    return table.reset_index(drop=True)


def parse_type2(pdf, read_file, website, pdf_url):
    """Parse type2 PDF of Scor.

    Keyword Arguments:
    pdf-- PdfFileReader() object
    read_file-- file name
    website-- fund_name_website
    pdf_url-- PDF file link
    """
    pages = []
    for i in range(pdf.getNumPages() - 1, 0, -1):
        k = i
        if "Designation of securities Currency Qty, number or nominal Market value" in pdf.getPage(
            k
        ).extractText().replace(
            "\n", ""
        ):
            while "Designation of securities Currency Qty, number or nominal Market value" in pdf.getPage(
                k
            ).extractText().replace(
                "\n", ""
            ):
                pages.append(k + 1)
                k -= 1
            break
    pages = sorted(pages)
    print(pages)
    tables = tabula.read_pdf(
        read_file,
        pages=pages,
        area=[0, 0, 828, 590],
        columns=[369, 391, 484, 543],
        guess=False,
        silent=True,
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
    table = table[~table["net_assets"].isna()]
    text = PdfFileReader(read_file).getPage(0).extractText().replace("\n", "")
    table["fund_name_report"] = (
        re.search(r"Annual report (.*) \d", text).group(1).strip()
    )
    table["fund_name_website"] = website
    table["pdf_url"] = pdf_url
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
            links_to_pdf_map[link] = "scor" + str(num) + ".pdf"
            with open("scor" + str(num) + ".pdf", "wb") as f:
                print("scor" + str(num) + ".pdf")
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
    for idx, row in pdf_links_df.iterrows():
        website = row["name"]
        read_file = row["files"]
        pdf_url = row["pdf_url"]
        pdf = PdfFileReader(read_file)
        pdf._override_encryption = True
        pdf._flatten()
        if "SCOR FUNDS" in pdf.getPage(0).extractText().replace("\n", ""):
            print("parsing ", read_file, website)
            tables.append(parse_type1(pdf, read_file, website, pdf_url))
        else:
            print("parsing ", read_file, website)
            tables.append(parse_type2(pdf, read_file, website, pdf_url))

    result = pd.concat(tables, ignore_index=True)
    result["currency"] = result["currency"].apply(lambda x: str(x).strip())
    to_keep = ["Cash at bank", "Other assets and liabilities"]
    result = result[
        result["currency"].isin(currencies)
        | result["holding_name"].isin(to_keep)
    ]
    result = result[~result["net_assets"].isna()]
    result = result.reset_index(drop=True)
    result["market_value"] = result["market_value"].apply(
        lambda x: "0.0" if x == "-" else x
    )
    result["net_assets"] = result["net_assets"].apply(
        lambda x: "0.0" if x == "-" else x
    )
    result["market_value"] = result["market_value"].apply(
        lambda x: float(x.replace(",", "").replace(" ", ""))
    )
    result["net_assets"] = result["net_assets"].apply(
        lambda x: float(x.replace(",", "."))
    )
    result = result[~result["holding_name"].isna()]
    for i, r in result.iterrows():
        if str(r["currency"]) == "nan":
            result["currency"][i] = result["currency"][i - 1]
    result["holding_name"] = result["holding_name"].apply(lambda x: x.strip())
    result["fund_provider"] = "SCOR Investment Partners"
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
