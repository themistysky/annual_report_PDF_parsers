"""Takes CSV as input. Downloads the PDF files and.

Parses PDFs of provider Mirova. Generates a CSV file
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

with open("./src/mirova/currencies.txt") as f:
    currencies = f.readline().split()


def parse_type1(pdf, read_file, website, pdf_url, isin):
    """Parse type1 PDF of Mirova.

    Keyword Arguments:
    pdf-- PdfFileReader() object
    read_file-- file name
    website-- fund_name_website
    pdf_url-- PDF file link
    isin-- ISIN
    """
    if read_file in file_contents:
        contents = file_contents[read_file]
    else:
        contents = tabula.read_pdf(
            read_file,
            pages=[2, 3],
            area=[0, 0, 828, 590],
            stream=True,
            columns=[530],
            guess=False,
            silent=True,
        )
        contents = [x for x in contents if x.shape[1] == 2]
        for i in range(len(contents)):
            contents[i].columns = ["fund", "pg"]
        contents = pd.concat(contents, ignore_index=True)
        contents = contents.dropna()
        contents = contents.reset_index(drop=True)
    start, end = 0, 0
    for idx, r in contents.iterrows():
        if (
            website.lower()[: website.find("Fund") - 1].replace("&", " and ")
            in r["fund"].lower()
        ):
            start, end = (
                int(r["pg"].split()[-1].strip()),
                int(contents["pg"][idx + 1].split()[-1].strip()) - 1,
            )
            break
    if not (start and end):
        print(read_file, website, "not found")
        return pd.DataFrame(
            columns=["holding_name", "currency", "market_value", "net_assets"]
        )
    pages = [
        i + 1
        for i in range(start + 1, end)
        if "Securities portfolio as at"
        in pdf.getPage(i).extractText().replace("\n", "")
    ]
    print(pages)
    tables = tabula.read_pdf(
        read_file,
        pages=pages,
        area=[0, 0, 828, 590],
        columns=[86, 354, 373, 533],
        guess=False,
        silent=True,
    )
    for i in range(len(tables)):
        tables[i].columns = [
            "_1",
            "holding_name",
            "currency",
            "market_value",
            "net_assets",
        ]
        tables[i].drop(columns=["_1"], inplace=True)
    table = pd.concat(tables, ignore_index=True)
    table = table[~table["net_assets"].isna()]
    table["fund_name_report"] = re.sub(r" ([A-Z] )?\([A-Z]\) .*$", "", website)
    table["fund_name_website"] = website
    table["pdf_url"] = pdf_url
    table["isin"] = isin
    return table.reset_index(drop=True)


def parse_type2(pdf, read_file, website, pdf_url, isin):
    """Parse type1 PDF of Mirova.

    Keyword Arguments:
    pdf-- PdfFileReader() object
    read_file-- file name
    website-- fund_name_website
    pdf_url-- PDF file link
    """
    pages = [
        i + 1
        for i in range(pdf.getNumPages())
        if "Désignation des valeurs"
        in pdf.getPage(i).extractText().replace("\n", "")
    ]
    print(pages)
    tables = tabula.read_pdf(
        read_file,
        pages=pages,
        area=[0, 0, 828, 590],
        columns=[324, 351, 418, 496],
        guess=False,
        silent=True,
    )
    for i in range(len(tables)):
        tables[i].columns = [
            "holding_name",
            "currency",
            "_3",
            "market_value",
            "net_assets",
        ]
        tables[i].drop(columns=["_3"], inplace=True)
    table = pd.concat(tables, ignore_index=True)
    table = table[~table["net_assets"].isna()]
    report = re.sub(r" ([A-Z] )?\([A-Z]\) .*$", "", website).upper()
    table["fund_name_website"] = website
    table["fund_name_report"] = report
    table["pdf_url"] = pdf_url
    table["isin"] = isin
    return table.reset_index(drop=True)


file_contents = {}


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
            links_to_pdf_map[link] = "mirova" + str(num) + ".pdf"
            with open("mirova" + str(num) + ".pdf", "wb") as f:
                print("mirova" + str(num) + ".pdf")
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
        isin = row["isin"]
        pdf = PdfFileReader(read_file)
        pdf._override_encryption = True
        pdf._flatten()
        if re.sub(
            r" ([A-Z] )?\([A-Z]\) .*$", "", website
        ).upper() in pdf.getPage(0).extractText().replace("\n", ""):
            print("parsing ", read_file, website)
            tables.append(parse_type2(pdf, read_file, website, pdf_url, isin))
        else:
            print("parsing ", read_file, website)
            tables.append(parse_type1(pdf, read_file, website, pdf_url, isin))
    result = pd.concat(tables, ignore_index=True)
    result["currency"] = result["currency"].apply(lambda x: str(x).strip())
    result = result[result["currency"].isin(currencies)]
    result = result[~result["net_assets"].isna()]
    result = result.reset_index(drop=True)
    result["market_value"] = result["market_value"].apply(
        lambda x: "0.0" if x == "-" else x
    )
    result["net_assets"] = result["net_assets"].apply(
        lambda x: "0.0" if x == "-" else x
    )
    for i, r in result.iterrows():
        x = r["market_value"].replace(" ", "")
        if "." in x and "," in x:
            result["market_value"][i] = float(x.replace(",", ""))
        else:
            result["market_value"][i] = float(x.replace(",", "."))
    result["net_assets"] = result["net_assets"].apply(
        lambda x: float(x.replace(",", "."))
    )
    result = result[~result["holding_name"].isna()]
    result["holding_name"] = result["holding_name"].apply(lambda x: x.strip())
    result["fund_provider"] = "MIROVA"
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
