"""Takes CSV as input. Downloads the PDF files and.

Parses PDFs of provider Mandarin. Generates a CSV file
containing parsed results.
"""
import re
import sys
import os
import warnings
from PyPDF2 import PdfFileReader
import pandas as pd
import numpy as np
import requests
import tabula
from pdfminer.pdfparser import PDFParser
from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdfinterp import resolve1

warnings.filterwarnings("ignore")

with open("./src/mandarin/currencies.txt") as f:
    currencies = f.readline().split()


def parse_type1(pdf, read_file, website, pdf_url):
    """Parse type1 PDF of Mandarin.

    Keyword Arguments:
    pdf-- PdfFileReader() object
    read_file-- file name
    website-- fund_name_website
    pdf_url-- PDF file link
    """
    text = "Inventaire des instruments financiers"
    pages = [
        i + 1
        for i in range(pdf.getNumPages())
        if text in pdf.getPage(i).extractText().replace("\n", "")
    ]
    tables = tabula.read_pdf(
        read_file,
        pages=pages,
        area=(145, 43, 830, 571),
        columns=(277, 328, 376, 410, 513),
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
    table = pd.concat(tables, ignore_index=True)
    table["fund_name_website"] = website
    table["fund_name_report"] = website.upper()
    table["pdf_url"] = pdf_url
    return table


def parse_type2(pdf, read_file, website, pdf_url):
    """Parse type2 PDF of Mandarin.

    Keyword Arguments:
    pdf-- PdfFileReader() object
    read_file-- file name
    website-- fund_name_website
    pdf_url-- PDF file link
    """
    pdf = PdfFileReader(read_file)
    pdf._override_encryption = True
    pdf._flatten()
    text1 = website.upper()
    text2 = "Portefeuille-Titres au"
    file = open(read_file, "rb")
    parser = PDFParser(file)
    document = PDFDocument(parser)
    tot_pg = resolve1(document.catalog["Pages"])["Count"]
    pages = [
        i + 1
        for i in range(tot_pg)
        if text1 in pdf.getPage(i).extractText().replace("\n", "")
        and text2 in pdf.getPage(i).extractText().replace("\n", "")
    ]
    pages.pop(0)
    report = re.search(r"\n(.*)\n", pdf.getPage(pages[0]).extractText()).group(
        1
    )
    tables = tabula.read_pdf(
        read_file,
        pages=pages,
        columns=(76, 207, 222, 271, 299, 344, 476, 491, 539),
        silent=True,
        guess=False,
        stream=True,
    )
    for i in range(len(tables)):
        df1, df2 = tables[i].iloc[:, :5], tables[i].iloc[:, 5:]
        df1.columns = df2.columns = [
            "_1",
            "holding_name",
            "currency",
            "market_value",
            "net_assets",
        ]
        df1.drop(columns=["_1"], inplace=True)
        df2.drop(columns=["_1"], inplace=True)
        df = pd.concat([df1, df2], ignore_index=True)
        tables[i] = df
    table = pd.concat(tables, ignore_index=True)
    table["fund_name_website"] = website
    table["fund_name_report"] = re.sub(r"[^a-zA-Z)( -]", "", report)
    table["pdf_url"] = pdf_url
    return table


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
            links_to_pdf_map[link] = "mandarin" + str(num) + ".pdf"
            with open("mandarin" + str(num) + ".pdf", "wb") as f:
                print("mandarin" + str(num) + ".pdf")
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
        if website in pdf.getPage(0).extractText().replace("\n", ""):
            print("parsing ", read_file, website)
            tables.append(parse_type1(pdf, read_file, website, pdf_url))
        else:
            print("parsing ", read_file, website)
            tables.append(parse_type2(pdf, read_file, website, pdf_url))
    result = pd.concat(tables, ignore_index=True)
    result = result[result["currency"].isin(currencies)]
    result = result.reset_index(drop=True)
    result["market_value"] = result["market_value"].apply(
        lambda x: "0.0" if x == "-" else x
    )
    result["net_assets"] = result["net_assets"].apply(
        lambda x: "0.0" if x == "-" else x
    )
    result["market_value"] = result["market_value"].apply(
        lambda x: float(x.replace(",", ""))
    )
    result["net_assets"] = result["net_assets"].apply(lambda x: float(x))
    result = result[~(result["net_assets"] == 100.00)]
    result["holding_name"] = result["holding_name"].apply(lambda x: x.strip())
    result["currency"] = result["currency"].apply(lambda x: x.strip())
    result["fund_provider"] = "Mandarine gestion"
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
