"""Takes CSV as input. Downloads the PDF files and.

Parses PDFs of provider Crelan. Generates a CSV file
containing parsed results.
"""
import re
import sys
import os
import warnings
from pdfminer.high_level import extract_text
from PyPDF2 import PdfFileReader as Read
import tabula as tb
import pandas as pd
import numpy as np
import requests
import time
import datetime

warnings.filterwarnings("ignore")

with open("./src/crelan/currencies.txt") as f:
    currencies = f.readline().split()

currencies.extend(["CNH", "CHN"])


def parse_crelan(
    pdf, read_file, website, columns=[257, 327, 356, 393, 457, 531]
):
    """Parse PDF of Crelan.

    Keyword Arguments:
    pdf -- PdfFileReader() Object
    read_file -- file name
    website -- fund name website
    columns -- column measurements of table in pts.
    """
    if read_file in file_contents:
        contents = file_contents[read_file]
    else:
        contents = tb.read_pdf(
            read_file,
            pages=[2, 3, 4, 5],
            area=[0, 50, 835, 590],
            columns=[544],
            guess=False,
            silent=True,
        )
        contents = [x for x in contents if x.shape[1] == 2]
        for i in range(len(contents)):
            contents[i].columns = ["fund", "pg"]
        contents = pd.concat(contents, ignore_index=True)
        contents = contents.dropna()
        contents = contents[contents["pg"].apply(lambda x: type(x) == float)]
        contents = contents.reset_index(drop=True)
        file_contents[read_file] = contents
    start, end = 0, 0
    pattern = re.compile(
        r"crelan fund |crelan invest |crelan pension |metropolitan rentastro | \(cap\)| \(dis\)"
    )
    for i, r in contents.iterrows():
        if (
            re.sub(pattern, "", website.lower()).split("-")[0]
            in r["fund"].lower()
        ):
            break
    if i == len(contents) - 1:
        i = 0
    pattern1 = any(
        ["Composition des actifs au" in x for x in contents["fund"]]
    )
    for idx, r in contents[i:].iterrows():
        if pattern1:
            if "Composition des actifs au" in r["fund"]:
                if idx == len(contents) - 1:
                    start, end = int(contents["pg"][idx]), min(
                        int(contents["pg"][idx]) + 12, pdf.getNumPages()
                    )
                else:
                    start, end = int(r["pg"]), int(contents["pg"][idx + 1])
                break
        else:
            if "COMPOSITION DES ACTIFS ET CHIFFRES-CLES" in r["fund"]:
                if idx == len(contents) - 1:
                    start, end = int(contents["pg"][idx]), min(
                        int(contents["pg"][idx]) + 12, pdf.getNumPages()
                    )
                else:
                    start, end = int(r["pg"]), int(contents["pg"][idx + 1])
                break
    if not (start and end):
        print("not found")
        return []
    print((start, end))
    pages = tuple(range(start, end + 1))
    tables = tb.read_pdf(
        read_file,
        pages=pages,
        area=[0, 0, 835, 590],
        columns=columns,
        guess=False,
        silent=True,
    )
    _tables = []
    for i in range(len(tables)):
        if len(tables[i].columns) == 7:
            tables[i].columns = [
                "holding_name",
                "_1",
                "currency",
                "_2",
                "market_value",
                "_3",
                "net_assets",
            ]
            tables[i] = tables[i].drop(columns=["_1", "_2", "_3"])
            _tables.append(tables[i])

    table = pd.concat(_tables, ignore_index=True)
    table["fund_name_report"] = website.split("-")[0]
    to_keep = ["CREANCES ET DETTES DIVERSES", "AUTRES"]
    table = table[
        table["currency"].isin(currencies)
        | table["holding_name"].isin(to_keep)
    ]
    table["net_assets"] = table["net_assets"].apply(
        lambda x: str(x).replace(",", ".").replace("%", "")
    )
    table["market_value"] = table["market_value"].apply(
        lambda x: x.replace(".", "").replace(",", ".").replace("–", "-")
    )
    if table["net_assets"].astype(float).sum() < 15.0:
        return parse_crelan(
            pdf, read_file, website, columns=[297, 366, 391, 429, 494, 532]
        )
    report = re.sub(pattern, "", website.lower()).split("-")[0]
    table["fund_name_report"] = report
    return table.reset_index(drop=True)


def parse_bnp(pdf, read_file, website):
    """Parse PDF of BNP.

    Keyword Arguments:
    pdf -- PdfFileReader() Object
    read_file -- file name
    website -- fund name website
    """
    if read_file in file_contents:
        contents = file_contents[read_file]
    else:
        contents = tb.read_pdf(
            read_file,
            pages=[2, 3, 4],
            area=[0, 50, 835, 590],
            columns=[547],
            guess=False,
            silent=True,
        )
        contents = [x for x in contents if x.shape[1] == 2]
        for i in range(len(contents)):
            contents[i].columns = ["fund", "pg"]
        contents = pd.concat(contents, ignore_index=True)
        contents = contents.dropna()
        contents = contents[contents["pg"].apply(lambda x: type(x) == float)]
        contents = contents.reset_index(drop=True)
        file_contents[read_file] = contents
    start, end = 0, 0
    for i, r in contents.iterrows():
        if website.lower().split("-")[0] in r["fund"].lower():
            break
    if i == len(contents) - 1:
        i = 0
    pattern1 = any(
        ["Composition des actifs au" in x for x in contents["fund"]]
    )
    for idx, r in contents[i:].iterrows():
        if pattern1:
            if "Composition des actifs au" in r["fund"]:
                if idx == len(contents) - 1:
                    start, end = int(contents["pg"][idx]), min(
                        int(contents["pg"][idx]) + 12, pdf.getNumPages()
                    )
                else:
                    start, end = int(r["pg"]), int(contents["pg"][idx + 1])
                break
        else:
            if "COMPOSITION DES ACTIFS ET CHIFFRES-CLES" in r["fund"]:
                if idx == len(contents) - 1:
                    start, end = int(contents["pg"][idx]), min(
                        int(contents["pg"][idx]) + 12, pdf.getNumPages()
                    )
                else:
                    start, end = int(r["pg"]), int(contents["pg"][idx + 1])
                break
    if not (start and end):
        print("not found")
        return []
    print((start, end))
    pages = tuple(range(start, end + 1))
    tables = tb.read_pdf(
        read_file,
        pages=pages,
        area=[0, 0, 835, 590],
        columns=[251, 317, 353, 397, 468, 540],
        guess=False,
        silent=True,
    )
    _tables = []
    for i in range(len(tables)):
        if len(tables[i].columns) == 7:
            tables[i].columns = [
                "holding_name",
                "_1",
                "currency",
                "_2",
                "market_value",
                "_3",
                "net_assets",
            ]
            tables[i] = tables[i].drop(columns=["_1", "_2", "_3"])
            _tables.append(tables[i])
    table = pd.concat(_tables, ignore_index=True)
    table["fund_name_report"] = website.split("-")[0]
    to_keep = ["CREANCES ET DETTES DIVERSES", "AUTRES"]
    table = table[
        table["currency"].isin(currencies)
        | table["holding_name"].isin(to_keep)
    ]
    table["net_assets"] = table["net_assets"].apply(
        lambda x: str(x).replace(",", ".")
    )
    table["market_value"] = table["market_value"].apply(
        lambda x: x.replace(".", "").replace(",", ".").replace("–", "-")
    )
    report = website.lower().split("-")[0]
    table["fund_name_report"] = report
    return table.reset_index(drop=True)


def parse_cpr(pdf, read_file, website):
    """Parse PDF of CPR.

    Keyword Arguments:
    pdf -- PdfFileReader() Object
    read_file -- file name
    website -- fund name website
    """
    if read_file in file_contents:
        contents = file_contents[read_file]
    else:
        contents = tb.read_pdf(
            read_file,
            pages=[2, 3],
            area=[0, 0, 828, 590],
            stream=True,
            columns=[495],
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
        if (
            website.split(" - ")[0].replace("CPR Invest ", "").strip().lower()
            in r["fund"].lower()
        ):
            start, end = int(r["pg"].split()[-1].strip()), int(
                contents["pg"][idx + 1].split()[-1].strip()
            )
            break
    if not (start and end):
        print("not found")
        return []
    pages = tuple(
        (
            i + 1
            for i in range(start + 1, end)
            if "Securities portfolio as at"
            in pdf.getPage(i).extractText().replace("\n", "")
        )
    )
    print((pages[0], pages[-1]))
    tables = tb.read_pdf(
        read_file,
        pages=pages,
        area=[0, 0, 828, 590],
        columns=[85, 352, 379, 532],
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
        tables[i] = tables[i].drop(columns=["_1"])
    table = pd.concat(tables, ignore_index=True)
    table = table[~table["net_assets"].isna()]
    table["fund_name_report"] = (
        website.split(" - ")[0].replace("CPR Invest ", "").strip()
    )
    table = table[table["currency"].isin(currencies)]
    table["market_value"] = table["market_value"].apply(
        lambda x: x.replace(",", "").replace("–", "-")
    )
    table["net_assets"] = table["net_assets"].apply(
        lambda x: x.replace("–", "-")
    )
    report = website.split(" - ")[0].replace("CPR Invest ", "").strip()
    table["fund_name_report"] = report
    return table.reset_index(drop=True)


def parse_amundi(pdf, read_file, website):
    """Parse PDF of Amundi.

    Keyword Arguments:
    pdf -- PdfFileReader() Object
    read_file -- file name
    website -- fund name website
    """
    if read_file in file_contents:
        contents = file_contents[read_file]
    else:
        cont_pgs = [
            i + 1
            for i in range(2, 6)
            if "Table"
            in extract_text(read_file, page_numbers=[i]).replace("\n", "")
        ]
        dfs = tb.read_pdf(
            read_file,
            pages=cont_pgs,
            area=[0, 0, 828, 590],
            columns=[268, 296, 527],
            silent=True,
            guess=False,
        )
        contents = []
        for i in range(len(dfs)):
            df1, df2 = dfs[i].iloc[:, :2], dfs[i].iloc[:, 2:]
            if len(df1.columns) == 2:
                df1.columns = ["fund", "pg"]
                contents.append(df1)
            if len(df2.columns) == 2:
                df2.columns = ["fund", "pg"]
                contents.append(df2)
        contents = pd.concat(
            [x for x in contents if x.shape[1] == 2], ignore_index=True
        )
        contents.dropna(inplace=True)
        contents.reset_index(drop=True, inplace=True)
        file_contents[read_file] = contents
    start, end = 0, 0
    for idx, r in contents.iterrows():
        if (
            website.split(" - ")[0].replace("Amundi Funds ", "").lower()
            in r["fund"].lower()
        ):
            start, end = int(r["pg"]), int(contents["pg"][idx + 1])
            break
    if not (start and end):
        print("not found")
        return []
    pages = tuple(range(start, end))
    print((start, end))
    tables = []
    df = tb.read_pdf(
        read_file,
        pages=pages,
        area=[0, 0, 828, 590],
        columns=[87, 229, 275, 300, 340, 489, 527],
        silent=True,
        guess=False,
    )
    for i in range(len(df)):
        df1, df2 = df[i].iloc[:, :4], df[i].iloc[:, 4:]
        if len(df1.columns) == 4:
            df1.columns = ["_1", "holding_name", "market_value", "net_assets"]
            tables.append(df1)
        if len(df2.columns) == 4:
            df2.columns = ["_1", "holding_name", "market_value", "net_assets"]
            tables.append(df2)
    table = pd.concat(tables, ignore_index=True)
    for x in table["market_value"]:
        if str(x).strip() in currencies:
            currency = str(x).strip()
            break
    table = table[
        table["_1"].apply(
            lambda x: str(x)
            .replace(",", "")
            .replace("-", "")
            .strip()
            .isdigit()
        )
    ]
    table.drop(columns=["_1"], inplace=True)
    table.dropna(inplace=True)
    table["fund_name_report"] = website.split(" - ")[0].replace(
        "Amundi Funds ", ""
    )
    table["currency"] = currency
    table["market_value"] = table["market_value"].apply(
        lambda x: x.replace(",", "").replace(" ", "")
    )
    table["net_assets"] = table["net_assets"].apply(
        lambda x: str(x).split()[0].replace(",", "")
    )
    report = website.split(" - ")[0].replace("Amundi Funds ", "")
    table["fund_name_report"] = report
    return table.reset_index(drop=True)


def parse_indosuez(pdf, read_file, website):
    """Parse PDF of Indosuez.

    Keyword Arguments:
    pdf -- PdfFileReader() Object
    read_file -- file name
    website -- fund name website
    """
    if read_file in file_contents:
        contents = file_contents[read_file]
    else:
        contents = tb.read_pdf(
            read_file,
            pages=[2, 3, 4, 5],
            area=[0, 50, 835, 590],
            columns=[517],
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
    pattern = "|".join(currencies) + "|Indosuez Funds "
    pattern = re.compile(pattern)
    report = re.sub(pattern, "", website.split(" - ")[0])
    for i, r in contents.iterrows():
        if report.lower() in r["fund"].lower():
            start, end = int(r["pg"]), int(contents["pg"][i + 1])
            break
    if not (start and end):
        print("not found")
        return []
    pages = tuple(range(start + 1, end))
    print((start, end))
    tables = tb.read_pdf(
        read_file,
        pages=pages,
        area=[0, 0, 828, 590],
        columns=[86, 353, 375, 533],
        guess=False,
        silent=True,
    )
    tables = [x for x in tables if x.shape[1] == 5]
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
    table = table[table["currency"].isin(currencies)]
    table["market_value"] = table["market_value"].apply(
        lambda x: str(x).replace(".", "").replace(",", ".")
    )
    table["fund_name_report"] = report
    table["net_assets"] = table["net_assets"].apply(
        lambda x: str(x).replace(",", ".")
    )
    return table.reset_index(drop=True)


def parse_econopolis(pdf, read_file, website):
    """Parse PDF of Econopolis.

    Keyword Arguments:
    pdf -- PdfFileReader() Object
    read_file -- file name
    website -- fund name website
    """
    if read_file in file_contents:
        contents = file_contents[read_file]
    else:
        contents = tb.read_pdf(
            read_file,
            pages=[2, 3, 4, 5],
            area=[0, 50, 835, 590],
            columns=[503],
            guess=False,
            silent=True,
        )
        contents = [x for x in contents if x.shape[1] == 2]
        for i in range(len(contents)):
            contents[i].columns = ["fund", "pg"]
        contents = pd.concat(contents, ignore_index=True)
        contents = contents.dropna()
        contents["pg"] = contents["pg"].apply(lambda x: x.replace(".", ""))
        contents = contents.reset_index(drop=True)
        file_contents[read_file] = contents
    start, end = 0, 0
    pattern = re.compile(r"Econopolis Funds | \(CAP\)| \(DIS\)")
    report = re.sub(pattern, "", website.split("-")[0]).strip()
    contents = contents.values.tolist()
    line = [x for x in contents if report.lower() in x[0].lower()][0]
    index = contents.index(line)

    for i in range(len(contents[index:])):
        if (
            "Statement of investments and other net assets"
            in contents[index + i][0]
        ):
            start, end = (
                int(contents[index + i][1].strip()) + 2,
                int(contents[index + i + 1][1].strip()) + 2,
            )
            break

    if not (start and end):
        print("not found")
        return []
    print((start, end))
    pages = tuple(range(start, end))
    print(pages)
    tables = tb.read_pdf(
        read_file,
        pages=pages,
        area=[0, 0, 828, 590],
        columns=[58, 129, 351, 440, 524],
        guess=False,
        silent=True,
    )
    tables = [x for x in tables if x.shape[1] == 6]
    for i in range(len(tables)):
        tables[i].columns = [
            "currency",
            "_1",
            "holding_name",
            "_2",
            "market_value",
            "net_assets",
        ]
        tables[i].drop(columns=["_1", "_2"], inplace=True)
    table = pd.concat(tables, ignore_index=True)
    to_keep = ["Cash at banks", "Other net assets/(liabilities)"]
    table["holding_name"] = table["holding_name"].apply(
        lambda x: str(x).strip()
    )
    table = table[
        table["currency"].isin(currencies)
        | table["holding_name"].isin(to_keep)
    ]
    table["market_value"] = table["market_value"].apply(
        lambda x: str(x).replace(",", "")
    )
    table["fund_name_report"] = report
    table["net_assets"] = table["net_assets"].apply(
        lambda x: str(x).replace(",", ".")
    )
    return table.reset_index(drop=True)


def parse_pricos(pdf, read_file, website):
    """Parse PDF of Pricos.

    Keyword Arguments:
    pdf -- PdfFileReader() Object
    read_file -- file name
    website -- fund name website
    """
    tables = tb.read_pdf(
        "crelan11.pdf", pages="all", stream=False, silent=True, guess=False
    )
    tables = [
        x.dropna(axis=1, how="all")
        for x in tables
        if x.dropna(axis=1, how="all").shape[1] >= 6
    ]
    for i in range(len(tables)):
        tables[i].loc[len(tables[i])] = list(tables[i].columns)
        if tables[i].shape[1] == 7:
            tables[i].columns = [
                "holding_name",
                "_1",
                "currency",
                "_2",
                "market_value",
                "_3",
                "net_assets",
            ]
            tables[i].drop(columns=["_1", "_2", "_3"], inplace=True)
        if tables[i].shape[1] == 6:
            tables[i].columns = [
                "holding_name",
                "_1",
                "currency",
                "_2",
                "market_value",
                "net_assets",
            ]
            tables[i].drop(columns=["_1", "_2"], inplace=True)
    print(" all")
    if len(tables) == 0:
        return 0
    table = pd.concat(tables, ignore_index=True)
    table["holding_name"] = table["holding_name"].apply(
        lambda x: str(x).strip()
    )
    to_keep = ["Te ontvangen interesten", "Te betalen kosten"]
    table = table[
        table["currency"].isin(currencies)
        | table["holding_name"].isin(to_keep)
    ]
    table["market_value"] = table["market_value"].apply(
        lambda x: str(x).replace(".", "").replace(",", ".")
    )
    table["net_assets"] = table["net_assets"].apply(
        lambda x: str(x).replace(",", ".")
    )
    table["fund_name_report"] = website
    return table.reset_index(drop=True)


def clean(text):
    """Clean the string passed (holding name).

    Keyword Arguments:
    text -- holding name
    """
    text = str(text)
    if text == "nan":
        return "_"
    text = re.sub(r"(?<!\d)%|[a-z]|%(?! )", "", text)
    text = re.sub(r"(?<=\d)[a-zA-Z](?! )|(?<=\.)[a-zA-Z](?! )", "", text)
    text = re.sub(r"(?<=[0-9/]) (?=[0-9/])", "", text)
    return text.strip()


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
            links_to_pdf_map[link] = "crelan" + str(num) + ".pdf"
            with open("crelan" + str(num) + ".pdf", "wb") as f:
                print("crelan" + str(num) + ".pdf")
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
    issues = []

    done_report = {}
    start = time.time()
    for idx, row in pdf_links_df.iterrows():
        table = []
        website = row["name"]
        read_file = row["files"]
        pattern = (
            "|".join(currencies)
            + "|Indosuez Funds "
            + "|Amundi Funds |Crelan Fund |Crelan Invest |Crelan Pension |Metropolitan Rentastro | \\(DIS\\)| \\(CAP\\)"
        )
        pattern = re.compile(pattern)
        report = re.sub(pattern, "", website.split(" - ")[0].split("-")[0])
        if str(read_file) == "nan":
            continue
        pdf_url = row["pdf_url"]
        isin = row["isin"]
        if report in done_report:
            print("parsing", read_file, website, "  memory")
            table = done_report[report].copy()
        else:
            if read_file in done:
                pdf = done[read_file]

            else:
                pdf = Read(read_file)
                done[read_file] = pdf

            pdf._override_encryption = True
            pdf._flatten()

            if "amundi" in website.lower():
                print("parsing ", read_file, website, end="  ")
                table = parse_amundi(pdf, read_file, website)
                done_report[report] = table.copy()
            elif "CPR" in website:
                print("parsing", read_file, website, end="  ")
                table = parse_cpr(pdf, read_file, website)
                done_report[report] = table.copy()
            elif "BNP" in website:
                print("parsing", read_file, website, end="  ")
                table = parse_bnp(pdf, read_file, website)
                done_report[report] = table.copy()
            elif "Crelan" in website or "Metropolitan Rentastro" in website:
                print("parsing ", read_file, website, end="  ")
                table = parse_crelan(pdf, read_file, website)
                done_report[report] = table.copy()
            elif "Indosuez" in website:
                print("parsing ", read_file, website, end="  ")
                table = parse_indosuez(pdf, read_file, website)
                done_report[report] = table.copy()
            elif "Econopolis" in website:
                print("parsing ", read_file, website, end="  ")
                table = parse_econopolis(pdf, read_file, website)
                done_report[report] = table.copy()
            elif "Pricos" in website:
                print("parsing ", read_file, website, end="  ")
                table = parse_pricos(pdf, read_file, website)
                done_report[report] = table.copy()
            else:
                continue

        if len(table) > 0:
            table["isin"] = isin
            table["fund_name_website"] = website
            table["pdf_url"] = pdf_url
            tables.append(table)
        else:
            issues.append((read_file, website))
    print("Time elapsed: ", datetime.timedelta(seconds=time.time() - start))

    tables = list(filter(lambda x: len(x) > 0, tables))
    result = pd.concat(tables, ignore_index=True)
    result["currency"] = result["currency"].apply(lambda x: str(x).strip())
    result["holding_name"] = result["holding_name"].apply(
        lambda x: str(x).strip()
    )
    for i, r in result.iterrows():
        if str(r["currency"]) == "nan":
            result["currency"][i] = result["currency"][i - 1]
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
    result = result[~result["net_assets"].apply(lambda x: str(x) == "nan")]
    result = result.reset_index(drop=True)
    result["market_value"] = result["market_value"].apply(
        lambda x: "0.0" if x == "-" else x
    )
    result = result[
        result["net_assets"].apply(lambda x: str(x).count(".") == 1)
    ]
    result["market_value"] = result["market_value"].apply(
        lambda x: float(
            "-"
            + re.sub(r"[)(]", "", str(x)).replace(",", ".").replace(" ", "")
        )
        if re.search(r"[)(]", x)
        else float(str(x).replace(",", ".").replace(" ", ""))
    )
    result["net_assets"] = result["net_assets"].apply(
        lambda x: float(
            "-"
            + re.sub(r"[)(]", "", str(x)).replace(",", ".").replace("%", "")
        )
        if re.search(r"[)(]", str(x))
        else float(str(x).replace(",", ".").replace("%", ""))
    )
    result = result[~result["holding_name"].isna()]
    result["holding_name"] = result["holding_name"].apply(clean)
    result["fund_provider"] = "Crelan"
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
