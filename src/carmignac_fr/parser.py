"""Takes CSV as input. Downloads the PDF files and.

Parses PDFs of provider Carmignac. Generates a CSV file
containing parsed results.
"""

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

with open("./src/carmignac_fr/currencies.txt") as f:
    currencies = f.readline().split()

_contents = []


def parse_type1(pdf, read_file, website, pdf_url):
    """Parse type 1 PDF of Carmignac(files with contents page).

    Keyword Arguments:
    pdf -- PdfFileReader() object
    read_file -- file name
    website -- fund_name_website
    pdf_url -- Pdf file link
    """
    global _contents
    if len(_contents) > 0:
        contents = _contents
    else:
        contents = tabula.read_pdf(
            read_file,
            pages=[2, 3],
            area=[0, 0, 828, 590],
            columns=[510],
            guess=False,
            silent=True,
        )
        contents[0].columns = contents[1].columns = ["fund", "pg"]
        contents = pd.concat(contents, ignore_index=True)
        contents = contents.dropna()
        _contents = contents
    start, end, report = 0, 0, None
    for idx, r in contents.iterrows():
        if website.lower() in " ".join(r["fund"].lower().split()):
            start, end = (
                int(r["pg"].split()[-1].strip()),
                int(contents["pg"][idx + 1].split()[-1].strip()) - 1,
            )
            break
        if "unconstrained" + website.lower().replace(
            "carmignac portfolio", ""
        ) in " ".join(r["fund"].lower().split()):
            start, end = (
                int(r["pg"].split()[-1].strip()),
                int(contents["pg"][idx + 1].split()[-1].strip()) - 1,
            )
            report = website.replace(
                "Carmignac Portfolio", "Carmignac Portfolio Unconstrained"
            )
            break
    if not (start and end):
        print("not found")
        return pd.DataFrame(
            columns=["holding_name", "currency", "market_value", "net_assets"]
        )
    pages = [
        i + 1
        for i in range(start + 1, end)
        if "Portefeuille-titres au"
        in pdf.getPage(i).extractText().replace("\n", "")
    ]
    print(pages)
    tables = tabula.read_pdf(
        read_file,
        pages=pages,
        area=[0, 0, 828, 590],
        columns=[89, 350, 411, 533],
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
    if report:
        table["fund_name_report"] = report
        table["fund_name_website"] = website
    else:
        table["fund_name_website"] = table["fund_name_report"] = website
    table["pdf_url"] = pdf_url
    return table.reset_index(drop=True)


def parse_type2(pdf, read_file, website, pdf_url):
    """Parse type 2 PDF of Carmignac(files with contents page).

    Keyword Arguments:
    pdf -- PdfFileReader() object
    read_file -- file name
    website -- fund_name_website
    pdf_url -- Pdf file link
    """
    pages = []
    pages = [
        i + 1
        for i in range(
            pdf.getNumPages() - 1, max(pdf.getNumPages() - 16, 0), -1
        )
        if "Désignation des valeurs"
        in pdf.getPage(i).extractText().replace("\n", "")
    ]
    if not pages:
        pages = [
            i + 1
            for i in range(
                pdf.getNumPages() - 1, max(pdf.getNumPages() - 16, 0), -1
            )
            if "Désignation des valeurs"
            in extract_text(read_file, page_numbers=[i])
        ]
    if not pages:
        print("not found")
        return pd.DataFrame(
            columns=["holding_name", "currency", "market_value", "net_assets"]
        )
    print(pages)
    tables = tabula.read_pdf(
        read_file,
        pages=pages[::-1],
        area=[0, 0, 828, 590],
        columns=[317, 346, 414, 494],
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
    table["fund_name_website"] = table["fund_name_report"] = website
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
            links_to_pdf_map[link] = "carmignac" + str(num) + ".pdf"
            with open("carmignac" + str(num) + ".pdf", "wb") as f:
                print("carmignac" + str(num) + ".pdf")
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
        if "CARMIGNAC PORTFOLIO" in pdf.getPage(0).extractText().replace(
            "\n", ""
        ):
            print("parsing ", read_file, website, end="  ")
            tables.append(parse_type1(pdf, read_file, website, pdf_url))
        else:
            print("parsing ", read_file, website, end="  ")
            tables.append(parse_type2(pdf, read_file, website, pdf_url))

    result = pd.concat(tables, ignore_index=True)
    result["currency"] = result["currency"].apply(lambda x: str(x).strip())
    to_keep = ["Créances", "Dettes", "Comptes financiers"]
    result = result[
        result["currency"].isin(currencies)
        | result["holding_name"].isin(to_keep)
    ]
    result["holding_name"] = result["holding_name"].apply(lambda x: x.strip())
    result = result[~result["net_assets"].isna()]
    result = result.reset_index(drop=True)
    result["market_value"] = result["market_value"].apply(
        lambda x: "0.0" if x == "-" else x
    )
    result["net_assets"] = result["net_assets"].apply(
        lambda x: "0.0" if x == "-" else x
    )
    result["market_value"] = result["market_value"].apply(
        lambda x: float(x.replace(",", ".").replace(" ", ""))
    )
    result["net_assets"] = result["net_assets"].apply(
        lambda x: float(x.replace(",", "."))
    )
    for i, r in result.iterrows():
        if str(r["currency"]) == "nan":
            result["currency"][i] = result["currency"][i - 1]
    result["holding_name"] = result["holding_name"].apply(lambda x: x.strip())
    result["isin"] = None
    result["fund_provider"] = "Carmignac (FR)"
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
