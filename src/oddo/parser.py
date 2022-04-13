"""Takes CSV as input. Downloads the PDF files and.

Parses PDFs of provider Oddo. Generates a CSV file
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

with open("./src/oddo/currencies.txt") as f:
    currencies = f.readline().split()
currencies.append("UNITÉ")
currencies.append("PART")
file_contents = {}
done1 = {}


def parse_type1(pdf, read_file, website, pdf_url, isin):
    """Parse type1 PDF of Oddo.

    Keyword Arguments:
    pdf -- PdfFileReader() object
    read_file -- file name
    website -- fund_name_website
    pdf_url -- PDF file link
    isin -- unique name for fund name
    """
    start, end = 0, 0
    if read_file in file_contents:
        contents = file_contents[read_file]
    else:
        contents = tabula.read_pdf(
            read_file,
            pages=[2, 3],
            area=[0, 0, 828, 590],
            stream=True,
            columns=[515],
            guess=False,
            silent=True,
        )
        contents = [x for x in contents if x.shape[1] == 2]
        for i in range(len(contents)):
            contents[i].columns = ["fund", "pg"]
        contents = pd.concat(contents, ignore_index=True)
        contents = contents.dropna()
        contents = contents.reset_index(drop=True)
        file_contents[read_file] = contents
    for idx, r in contents.iterrows():
        if (
            " ".join(website.split()[:-1])
            .replace("ODDO BHF ", "")
            .replace("Sustainable ", "")
            in r["fund"]
        ):
            start, end = (
                int(r["pg"].split()[-1].strip()),
                int(contents["pg"][idx + 1].split()[-1].strip()) - 1,
            )
            break
    if not (start and end):
        end = pdf.getNumPages()
    pages = tuple(
        (
            i + 1
            for i in range(start + 1, end)
            if "Etat des investissements au"
            in pdf.getPage(i).extractText().replace("\n", "")
        )
    )
    print(pages)
    if (read_file) in done1:
        table = done1[read_file]
    else:
        tables = tabula.read_pdf(
            read_file,
            pages=pages,
            area=[0, 0, 828, 590],
            columns=[354, 376, 475, 533],
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
            tables[i] = tables[i].drop(columns=["_2"])
        table = pd.concat(tables, ignore_index=True)
        done1[read_file] = table
    table = table[~table["net_assets"].isna()]
    table["fund_name_report"] = " ".join(website.split()[:-1])
    table["fund_name_website"] = website
    table["pdf_url"] = pdf_url
    table["isin"] = isin
    for i, r in table.iterrows():
        if r["currency"] in currencies:
            table["holding_name"][i] = " ".join(r["holding_name"].split()[1:])
    to_keep = [
        "Avoirs bancaires",
        "Autres actifs/passifs nets",
        "Liquidités",
        "Autres actifs/passifs",
        "TOTAL Lignes détenus en pleine propriété",
    ]
    table = table[
        table["currency"].isin(currencies)
        | table["holding_name"].isin(to_keep)
    ]
    table["market_value"] = table["market_value"].apply(
        lambda x: x.replace(".", "").replace(",", ".").replace("–", "-")
    )
    table["market_value"] = table["market_value"].apply(
        lambda x: "-" + re.sub(r"[)(]", "", x).replace(" ", "")
        if re.search(r"[)(]", x)
        else x.replace(" ", "")
    )
    table["net_assets"] = table["net_assets"].apply(
        lambda x: "-" + re.sub(r"[)(]", "", x).replace(",", ".")
        if re.search(r"[)(]", x)
        else x.replace(",", ".").replace(" ", "")
    )
    return table.reset_index(drop=True)


done2 = {}


def parse_type2(pdf, read_file, website, pdf_url, isin):
    """Parse type1 PDF of Oddo.

    Keyword Arguments:
    pdf -- PdfFileReader() object
    read_file -- file name
    website -- fund_name_website
    pdf_url -- PDF file link
    isin -- unique name for fund name
    """
    text1 = website[: website.find("(") - 1].replace("Exklusiv: ", "")
    text2 = "Répartition du portefeuille-titres au"
    pages = tuple(
        (
            i + 1
            for i in range(pdf.getNumPages())
            if text1 in pdf.getPage(i).extractText().replace("\n", "")
            and text2 in pdf.getPage(i).extractText().replace("\n", "")
        )
    )
    print(pages)
    if (pages, read_file) in done2:
        table = done2[(pages, read_file)]
    else:
        tables = tabula.read_pdf(
            read_file,
            pages=pages,
            area=[0, 0, 828, 590],
            columns=[354, 376, 475, 533],
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
        done2[(pages, read_file)] = table.copy()
    table = table[~table["net_assets"].isna()]
    table["fund_name_report"] = " ".join(website.split()[:-1])
    table["fund_name_website"] = website
    table["pdf_url"] = pdf_url
    table["isin"] = isin
    for i, r in table.iterrows():
        if r["currency"] in currencies:
            table["holding_name"][i] = " ".join(r["holding_name"].split()[1:])
    to_keep = [
        "Avoirs bancaires",
        "Autres actifs/passifs nets",
        "Liquidités",
        "Autres actifs/passifs",
        "TOTAL Lignes détenus en pleine propriété",
    ]
    table = table[
        table["currency"].isin(currencies)
        | table["holding_name"].isin(to_keep)
    ]
    table["market_value"] = table["market_value"].apply(
        lambda x: x.replace(".", "").replace(",", ".").replace("–", "-")
    )
    return table.reset_index(drop=True)


done3, not_done3 = {}, []


def parse_type3(pdf, read_file, website, pdf_url, isin):
    """Parse type1 PDF of Oddo.

    Keyword Arguments:
    pdf -- PdfFileReader() object
    read_file -- file name
    website -- fund_name_website
    pdf_url -- PDF file link
    isin -- unique name for fund name
    """
    pages = []
    if read_file in done3:
        table = done3[read_file]
        print()
    else:
        for i in range(pdf.getNumPages(), 0, -1):
            if "Désignation des valeurs" in extract_text(
                read_file, page_numbers=[i]
            ).replace("\n", ""):
                k = i
                while "Désignation des valeurs" in extract_text(
                    read_file, page_numbers=[k]
                ).replace("\n", ""):
                    pages.append(k + 1)
                    k -= 1
                break
        pages = tuple(sorted(pages))
        print(pages)
        tables = tabula.read_pdf(
            read_file,
            pages=pages,
            area=[0, 0, 828, 590],
            columns=[349, 383, 472, 528],
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
        done3[read_file] = table.copy()
    table.iloc[:, 2] = table.iloc[:, 2].apply(str) + table.iloc[
        :, 3
    ].copy().apply(
        lambda x: str(x).split()[0] if len(str(x).split()) > 1 else ""
    )
    table.iloc[:, 3] = table.iloc[:, 3].apply(lambda x: str(x).split()[-1])
    table = table[~table["net_assets"].isna()]
    report = re.sub(r" ([A-Z] )?\([A-Z]\) .*$", "", website).upper()
    table["fund_name_website"] = website
    table["fund_name_report"] = report
    table["pdf_url"] = pdf_url
    table["isin"] = isin
    to_keep = [
        "Avoirs bancaires",
        "Autres actifs/passifs nets",
        "Liquidités",
        "Autres actifs/passifs",
        "TOTAL Lignes détenus en pleine propriété",
    ]
    table = table[
        table["currency"].isin(currencies)
        | table["holding_name"].isin(to_keep)
    ]
    table["market_value"] = table["market_value"].apply(
        lambda x: x.replace(" ", "").replace(",", ".").replace("–", "-")
    )
    # table.iloc[:,-1] = table.iloc[:,-1].apply(lambda x: str(x).replace(' ',''))
    return table.reset_index(drop=True)


done4 = {}


def parse_type4(pdf, read_file, website, pdf_url, isin):
    """Parse type1 PDF of Oddo.

    Keyword Arguments:
    pdf -- PdfFileReader() object
    read_file -- file name
    website -- fund_name_website
    pdf_url -- PDF file link
    isin -- unique name for fund name
    """
    if read_file in done4:
        table = done4[read_file]
        print()
    else:
        if read_file in file_contents:
            contents = file_contents[read_file]
        else:
            contents = tabula.read_pdf(
                read_file,
                pages=[2, 3],
                area=[0, 0, 828, 590],
                stream=True,
                columns=[515],
                guess=False,
                silent=True,
            )
            contents = [x for x in contents if x.shape[1] == 2]
            for i in range(len(contents)):
                contents[i].columns = ["fund", "pg"]
            contents = pd.concat(contents, ignore_index=True)
            contents = contents.dropna()
            contents = contents.reset_index(drop=True)
            file_contents[read_file] = contents
        start, end = 0, 0
        for idx, r in contents.iterrows():
            if "État du patrimoine" in r["fund"]:
                start, end = int(r["pg"]), int(contents["pg"][idx + 1])
                break
        pages = list(range(start, end))
        print(pages)

        tables = tabula.read_pdf(
            read_file,
            pages=pages,
            area=[0, 0, 828, 590],
            columns=[197, 235, 450, 519],
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
        done4[read_file] = table
    table["currency"] = table["currency"].apply(lambda x: str(x).strip())
    table = table[table["currency"].isin(currencies)]
    table["market_value"] = table["market_value"].apply(
        lambda x: x.replace(".", "").replace(",", ".").replace("–", "-")
    )
    table["net_assets"] = table["net_assets"].apply(
        lambda x: x.replace(".", "").replace(",", ".").replace("–", "-")
    )
    report = " ".join(website.split()[:-1])
    table["fund_name_website"] = website
    table["fund_name_report"] = report
    table["pdf_url"] = pdf_url
    table["isin"] = isin
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
            links_to_pdf_map[link] = "oddo" + str(num) + ".pdf"
            with open("oddo" + str(num) + ".pdf", "wb") as f:
                print("oddo" + str(num) + ".pdf")
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

    _type = None
    tables = []
    done = {}
    for idx, row in pdf_links_df.iterrows():
        website = row["name"]
        read_file = row["files"]
        if str(read_file) == "nan":
            continue
        pdf_url = row["pdf_url"]
        isin = row["isin"]
        if read_file in done:
            pdf = done[read_file]
        else:
            try:
                pdf = PdfFileReader(read_file)
                done[read_file] = pdf
            except:
                continue
        page0 = pdf.getPage(0).extractText().replace("\n","")
        if "SICAV ODDO BHF" in page0:
            _type = 1
        elif "Rapport annuel révisé" in page0:
            _type = 2
        elif "RAPPORT ANNUEL" in page0:
            _type = 4
        else:
            _type = 3

        if _type == 1:
            print("parsing ", read_file, website, end="  ")
            tables.append(parse_type1(pdf, read_file, website, pdf_url, isin))
        elif _type == 2:
            print("parsing", read_file, website, end="  ")
            tables.append(parse_type2(pdf, read_file, website, pdf_url, isin))
        elif _type == 4:
            print("parsing", read_file, website, end="  ")
            tables.append(parse_type4(pdf, read_file, website, pdf_url, isin))
        else:
            print("parsing ", read_file, website, end="  ")
            tables.append(parse_type3(pdf, read_file, website, pdf_url, isin))

    result = pd.concat(tables, ignore_index=True)
    result["currency"] = result["currency"].apply(lambda x: str(x).strip())
    result["holding_name"] = result["holding_name"].apply(
        lambda x: str(x).strip()
    )
    to_keep = [
        "Avoirs bancaires",
        "Autres actifs/passifs nets",
        "Liquidités",
        "Autres actifs/passifs",
        "TOTAL Lignes détenus en pleine propriété",
    ]
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
    result = result[
        result["market_value"].apply(
            lambda x: str(x)
            .replace(",", "", 1)
            .replace(".", "", 1)
            .replace("-", "", 1)
            .isdigit()
        )
    ]
    result = result[
        result["market_value"].apply(
            lambda x: re.sub("[,. –-]", "", x).isdigit()
            and not re.search(r"\d-\d", x)
        )
    ]
    result["market_value"] = result["market_value"].apply(
        lambda x: float("-" + re.sub(r"[)(]", "", x).replace(" ", ""))
        if re.search(r"[)(]", x)
        else float(x.replace(" ", ""))
    )
    result["net_assets"] = result["net_assets"].apply(
        lambda x: float("-" + re.sub(r"[)(]", "", x).replace(",", "."))
        if re.search(r"[)(]", x)
        else float(x.replace(",", ".").replace(" ", ""))
    )
    result = result[~result["holding_name"].isna()]
    for i, r in result.iterrows():
        if str(r["currency"]) == "nan":
            result["currency"][i] = result["currency"][i - 1]
    result["holding_name"] = result["holding_name"].apply(
        lambda x: "_" if x == "nan" else x
    )
    result["fund_provider"] = "ODDO BHF Asset Management SAS (FR)"
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
