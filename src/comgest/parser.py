"""Takes CSV as input. Downloads the PDF files and.
Parses PDFs of provider Comgest. Generates a CSV file
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

with open("./src/comgest/currencies.txt") as f:
    currencies = f.readline().split()

file_contents = {}


def parse_type1(pdf, read_file, website, pdf_url):
    """Parse type1 PDF of Comgest.
    Keyword Arguments:
    pdf-- PdfFileReader() object
    read_file-- file name
    website-- fund_name_website
    pdf_url-- PDF file link
    """
    if read_file in file_contents:
        contents = file_contents[read_file]
    else:
        cont_pg = []
        for i in range(5):
            if "TABLE DES MATIERES" in pdf.getPage(i).extractText().replace(
                "\n", ""
            ):
                k = i
                while (
                    "Mouvements Significatifs Du Portefeuille"
                    not in pdf.getPage(k).extractText().replace("\n", "")
                ):
                    cont_pg.append(k + 1)
                    k += 1
                cont_pg.append(k + 1)
                break
        contents = tabula.read_pdf(
            read_file,
            pages=cont_pg,
            area=[0, 0, 828, 590],
            columns=[530],
            silent=True,
            guess=False,
        )
        for i in range(len(contents)):
            contents[i].columns = ["fund", "pg"]
        contents = pd.concat(contents, ignore_index=True)
        contents.dropna(inplace=True)

        def clean(x):
            return str(x).replace(".", "").strip()

        contents["fund"] = contents["fund"].apply(clean)
        contents["pg"] = contents["pg"].apply(clean)
        contents = contents[
            : contents[
                contents["fund"] == "Mouvements Significatifs Du Portefeuille"
            ].index[0]
        ]
        contents.reset_index(drop=True, inplace=True)
        file_contents[read_file] = contents
    to_remove = currencies[:] + ["FIXED", "SI", "SU"]
    to_remove = ["(?<= )" + x + "(?= )" for x in to_remove]
    pattern = re.compile(
        "|".join(to_remove) + "|(?<= )[A-Z](?= )" + "|ACC|DIS"
    )
    text = re.sub(pattern, "", website)
    report = re.sub("\\s+", " ", text).strip()
    start, end = 0, 0
    for idx, r in contents.iterrows():
        if report in r["fund"]:
            start, end = int(r["pg"]) - 2, int(contents["pg"][idx + 1]) + 7
            break
    if not (start and end):
        print("not found")
        return []

    def func(x, i):
        return x.getPage(i).extractText().replace("\n", "")

    text = "Juste valeur"
    pattern1 = re.compile(
        report.lower() + ".?\\d?\\s+" + "état du portefeuille au"
    )

    def func1(i):
        return tabula.read_pdf(
            read_file,
            pages=[i],
            area=[121, 0, 176, 590],
            silent=True,
            guess=False,
            stream=True,
        )[0]

    pages = []
    strt = True
    for i in range(start, end + 6):
        if re.search(pattern1, func(pdf, i).lower()):
            if strt:
                pages.append(i + 1)
                strt = False
                continue
            col_text = func1(i + 1).to_string()
            if text in col_text or "Valeur    nominale" in col_text:
                pages.append(i + 1)
    pages = tuple(pages)
    print(pages)
    tables = tabula.read_pdf(
        read_file,
        pages=pages,
        area=[0, 0, 828, 590],
        columns=[300, 349, 408, 462, 528],
        stream=True,
        guess=False,
        silent=True,
    )
    for i in range(len(tables)):
        tables[i].columns = [
            "holding_name",
            "_2",
            "currency",
            "_4",
            "market_value",
            "net_assets",
        ]
        tables[i] = tables[i].drop(columns=["_2", "_4"])
    table = pd.concat(tables, ignore_index=True)
    table = table[~table["net_assets"].isna()]
    table["fund_name_report"] = report
    table["pdf_url"] = pdf_url
    to_keep = [
        "Liquidités et autres actifs nets",
        "Total des instruments financiers dérivés (Note 6)",
    ]
    table["currency"] = table["currency"].apply(
        lambda x: re.sub(r"\d+", "", str(x)).strip()
    )
    table = table[
        table["currency"].isin(currencies)
        | table["holding_name"].isin(to_keep)
    ]
    table["market_value"] = table["market_value"].apply(
        lambda x: x.replace(" ", "").replace(",", ".").replace("–", "-")
    )
    table = table[
        table["net_assets"].apply(
            lambda x: x.replace(",", "").replace(" ", "").isdigit()
        )
    ]
    return table.reset_index(drop=True)


def parse_type2(pdf, read_file, website, pdf_url):
    """Parse type2 PDF of Comgest.
    Keyword Arguments:
    pdf-- PdfFileReader() object
    read_file-- file name
    website-- fund_name_website
    pdf_url-- PDF file link
    """
    to_remove = currencies[:] + ["FIXED", "SI", "SU"]
    to_remove = ["(?<= )" + x + "(?= )" for x in to_remove]
    pattern = re.compile(
        "|".join(to_remove) + "|(?<= )[A-Z](?= )" + "|ACC|DIS"
    )
    text = re.sub(pattern, "", website)
    report = re.sub("\\s+", " ", text).strip()
    pages = []
    for i in range(pdf.getNumPages() - 1, 0, -1):
        if "Désignation des valeurs" in pdf.getPage(i).extractText().replace(
            "\n", ""
        ):
            k = i
            while "Désignation des valeurs" in pdf.getPage(
                k
            ).extractText().replace("\n", ""):
                pages.append(k + 1)
                k -= 1
            break
    pages = tuple(sorted(pages))
    print(pages)
    tables = tabula.read_pdf(
        read_file,
        pages=pages,
        area=[0, 0, 828, 590],
        columns=[317, 355, 421, 501],
        guess=False,
        silent=True,
    )
    for i in range(len(tables)):
        tables[i].columns = [
            "holding_name",
            "currency",
            "_2",
            "market_value",
            "net_assets",
        ]
        tables[i].drop(columns=["_2"], inplace=True)
    table = pd.concat(tables, ignore_index=True)
    table = table[~table["net_assets"].isna()]
    table["fund_name_report"] = report
    table["pdf_url"] = pdf_url
    to_keep = ["Créances", "Dettes", "Comptes financiers"]
    table["currency"] = table["currency"].apply(
        lambda x: re.sub(r"\d+", "", str(x)).strip()
    )
    # print(table)
    table = table[
        table["currency"].isin(currencies)
        | table["holding_name"].isin(to_keep)
    ]
    table["market_value"] = table["market_value"].apply(
        lambda x: x.replace(" ", "").replace(",", ".").replace("–", "-")
    )
    table["net_assets"] = table["net_assets"].apply(
        lambda x: x if x.replace(",", "").replace(" ", "").isdigit() else "0"
    )
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
            links_to_pdf_map[link] = "comgest" + str(num) + ".pdf"
            with open("comgest" + str(num) + ".pdf", "wb") as f:
                print("comgest" + str(num) + ".pdf")
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
    issues = []
    done = {}
    to_remove = currencies[:] + ["FIXED", "SI", "SU"]
    to_remove = ["(?<= )" + x + "(?= )" for x in to_remove]
    pattern = re.compile(
        "|".join(to_remove) + "|(?<= )[A-Z](?= )" + "|ACC|DIS"
    )
    type1_file = None
    for idx, row in pdf_links_df.iterrows():
        website = row["name"]
        text = re.sub(pattern, "", website)
        report = re.sub("\\s+", " ", text).strip()
        pdf_url = row["pdf_url"]
        read_file = row["files"]
        if str(read_file) in ["nan"]:
            continue
        if report in done:
            table = done[report].copy()
            print("parsing", read_file, website, "  memory")
        elif read_file in done:
            table = done[read_file].copy()
            print("parsing", read_file, website, "  memory")
        else:
            pdf = PdfFileReader(read_file)
            if "COMGEST GROWTH PLC" in pdf.getPage(0).extractText().replace(
                "\n", ""
            ):
                if not type1_file: type1_file = read_file
                else: read_file = type1_file
                print("parsing1 ", read_file, website, end="  \n")
                table = parse_type1(pdf, read_file, website, pdf_url)
                done[report] = table
            else:
                continue
                print("parsing2", read_file, website, end="  \n")
                table = parse_type2(pdf, read_file, website, pdf_url)
                done[read_file] = table

        if len(table) > 0:
            table["fund_name_website"] = website
            tables.append(table)
        else:
            issues.append((read_file, website))

    tables = list(filter(lambda x: len(x) > 0, tables))
    result = pd.concat(tables, ignore_index=True)
    result["currency"] = result["currency"].apply(lambda x: str(x).strip())
    result["holding_name"] = result["holding_name"].apply(
        lambda x: str(x).strip()
    )
    to_keep = [
        "Liquidités et autres actifs nets",
        "Total des instruments financiers dérivés (Note 6)",
    ]
    result = result[
        result["currency"].isin(currencies)
        | result["holding_name"].isin(to_keep)
    ]
    result["market_value"] = result["market_value"].apply(
        lambda x: float(
            "-" + re.sub(r"[)(]", "", x).replace(",", ".").replace(" ", "")
        )
        if re.search(r"[)(]", x)
        else float(x.replace(",", ".").replace(" ", ""))
    )
    result["net_assets"] = result["net_assets"].apply(
        lambda x: float("-" + re.sub(r"[)(]", "", x).replace(",", "."))
        if re.search(r"[)(]", x)
        else float(x.replace(",", "."))
    )
    for i, r in result.iterrows():
        if str(r["currency"]) == "nan":
            result["currency"][i] = result["currency"][i - 1]
    result["holding_name"] = result["holding_name"].apply(
        lambda x: "_" if x == "nan" else x
    )
    result["fund_provider"] = "Comgest (FR)"
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
