"""Takes CSV as input. Downloads the PDF files and.

Parses PDFs of provider Roth. Generates a CSV file
containing parsed results.
"""
import re
import sys
import os
import warnings
import PyPDF2
from PyPDF2 import PdfFileReader
import tabula
import pandas as pd
import numpy as np
from selenium import webdriver
import shutil
from webdriver_manager.chrome import ChromeDriverManager
from pdfminer.pdfparser import PDFParser
from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdfinterp import resolve1
import time

warnings.filterwarnings("ignore")

with open("./src/roth/currencies.txt") as f:
    currencies = f.readline().split()


file_contents = {}

not_found = "not found"


def clean1(table, report, website, pdf_url, isin):
    """Clean dataframe parsed by parse_type3()."""
    table = table[~table["net_assets"].isna()]
    table["fund_name_report"] = report
    table["pdf_url"] = pdf_url
    to_keep = [
        "Liquidités/(découvert bancaire)",
        "Autres actifs/(passifs) nets",
    ]
    table["currency"] = table["currency"].apply(
        lambda x: re.sub(r"\d+", "", str(x)).strip()
    )
    table = table[
        table["currency"].isin(currencies)
        | table["holding_name"].isin(to_keep)
    ]
    table = table.reset_index(drop=True)
    if "visionfund" in website.lower():
        table["market_value"] = table["market_value"].apply(
            lambda x: "-"
            + re.sub(r"[)(]", "", x).replace(".", "").replace(",", ".")
            if re.search(r"[)(]", x)
            else x.replace(".", "").replace(",", ".")
        )
        table["net_assets"] = table["net_assets"].apply(
            lambda x: "-" + re.sub(r"[)(]", "", x).replace(",", ".")
            if re.search(r"[)(]", x)
            else x.replace(",", ".")
        )
    else:
        table["market_value"] = table["market_value"].apply(
            lambda x: "-"
            + re.sub(r"[)(]", "", x).replace(",", "").replace(" ", "")
            if re.search(r"[)(]", x)
            else x.replace(",", "").replace(" ", "")
        )
        table["net_assets"] = table["net_assets"].apply(
            lambda x: "-" + re.sub(r"[)(]", "", x).replace(",", ".")
            if re.search(r"[)(]", x)
            else x.replace(",", ".")
        )
    # currency value in rows with holding_name in to_keep (line 109)
    # have "nan" values. They are filled with the most occuring
    # currency value in table.
    mode = table.currency.mode()[0]
    for i, r in table.iterrows():
        if str(r["currency"]) == "nan":
            table["currency"][i] = mode
    table["fund_name_website"] = website
    table["isin"] = isin
    return table.reset_index(drop=True)


def parse_contents(read_file, pages_=[1, 2, 3, 4]):
    """Parse contents page of the file."""
    contents = tabula.read_pdf(
        read_file,
        pages=pages_,
        area=[0, 0, 828, 590],
        columns=[490],
        silent=True,
        guess=False,
    )
    contents = [x for x in contents if x.shape[1] == 2]
    for i in range(len(contents)):
        contents[i].columns = ["fund", "pg"]
    contents = pd.concat(contents, ignore_index=True)
    contents = contents.dropna()

    def clean(x):
        return str(x).replace(".", "").strip().lower()

    contents["fund"] = contents["fund"].apply(clean)
    contents["pg"] = contents["pg"].apply(clean)
    contents = contents[contents["pg"].apply(lambda x: str(x).isdigit())]
    contents = contents.reset_index(drop=True)
    return contents


def find_visionfund_pgs(func1, pdf, report, end):
    """Find page numbers of tables from PDF."""
    try:
        func1(10, pdf)
    except PyPDF2.utils.PdfReadError as e:
        print(type(e))
        return []
    pages, text = [], "QuantitDnominationValeurd'acqui-sition"
    for i in range(end):
        if (
            text in func1(i, pdf)
            and report.lower() in func1(i, pdf)[:35].lower()
        ):
            pages.append(i + 1)
    return pages


def find_pages1(read_file, website, contents, pdf, report):
    """Find page numbers of tables from PDF."""
    start, end = 0, 0
    for idx, r in contents.iterrows():
        if report.lower() in r["fund"].lower():
            start, end = int(r["pg"]), int(contents["pg"][idx + 1])
            break
    if "visionfund" in website.lower():
        try:
            end = pdf.getNumPages()
        except PyPDF2.utils.PdfReadError:
            parser = PDFParser(open(read_file, "rb"))
            document = PDFDocument(parser)
            end = resolve1(document.catalog["Pages"])["Count"]
    elif not (start and end):
        print(not_found)
        return []

    text = "ValeurnominaleDnominationValeurd'acqui"

    def func1(i, pdf):
        return pdf.getPage(i).extractText().replace("\n", "")

    if "visionfund" in website.lower():
        pages = find_visionfund_pgs(func1, pdf, report, end)
    else:
        pages = [i + 1 for i in range(start, end) if text in func1(i, pdf)]

    return tuple(pages)


def parse_type1(pdf, read_file, website, pdf_url, isin):
    """Parse type1 PDF of Roth.

    Keyword Arguments:
    pdf -- PdfFileReader() object
    read_file -- file name
    website -- fund name website
    pdf_url -- PDF link
    """
    if read_file in file_contents:
        contents = file_contents[read_file]
    else:
        cont_pg = [
            i + 1
            for i in [2, 3, 4]
            if "TABLE DES MATIERES"
            in pdf.getPage(i).extractText().replace("\n", "")
        ]
        if not cont_pg:
            return parse_type2(pdf, read_file, website, pdf_url, isin)

        contents = parse_contents(read_file, cont_pg)
        file_contents[read_file] = contents
    report = re.sub("EdR Fund|VisionFund -", "", website).strip()
    pages = find_pages1(read_file, website, contents, pdf, report)
    if not pages:
        return []
    print(pages)
    tables = tabula.read_pdf(
        read_file,
        pages=pages,
        area=[0, 0, 828, 590],
        columns=[107, 318, 344, 406, 455, 512],
        stream=True,
        guess=False,
        silent=True,
    )
    for i in range(len(tables)):
        tables[i].columns = [
            "_1",
            "holding_name",
            "currency",
            "_2",
            "_3",
            "market_value",
            "net_assets",
        ]
        tables[i] = tables[i].drop(columns=["_1", "_2", "_3"])
    table = pd.concat(tables, ignore_index=True)
    return clean1(table, report, website, pdf_url, isin)


def clean2(table, report, website, pdf_url, isin):
    """Clean dataframe parsed by parse_type2()."""
    table = table[~table["net_assets"].isna()]
    table["fund_name_report"] = report
    table["pdf_url"] = pdf_url
    to_keep = ["Cash/(bank overdraft)", "Other assets and liabilities"]
    table["currency"] = table["currency"].apply(
        lambda x: re.sub(r"\d+", "", str(x)).strip()
    )
    table = table[
        table["currency"].isin(currencies)
        | table["holding_name"].isin(to_keep)
    ]
    table = table.reset_index(drop=True)
    table["market_value"] = table["market_value"].apply(
        lambda x: "-"
        + re.sub(r"[)(]", "", x).replace(",", "").replace(" ", "")
        if re.search(r"[)(]", x)
        else x.replace(",", "").replace(" ", "")
    )
    table["net_assets"] = table["net_assets"].apply(
        lambda x: "-" + re.sub(r"[)(]", "", x).replace(",", ".")
        if re.search(r"[)(]", x)
        else x.replace(",", ".")
    )
    mode = table.currency.mode()[0]
    for i, r in table.iterrows():
        if str(r["currency"]) == "nan":
            table["currency"][i] = mode
    table["fund_name_website"] = website
    table["isin"] = isin
    return table.reset_index(drop=True)


def find_pages2(read_file, website, contents, pdf, report):
    """Find page numbers of tables from PDF."""
    start, end = 0, 0
    for idx, r in contents.iterrows():
        if report.lower() in r["fund"].lower().replace("  ", " "):
            start, end = int(r["pg"]), int(contents["pg"][idx + 1])
            text = "ValuepersecurityMarketvalue"

            def func1(i, pdf):
                return pdf.getPage(i).extractText().replace("\n", "")

            pages = [i + 1 for i in range(start, end) if text in func1(i, pdf)]
            break

    if not (start and end):
        parser = PDFParser(open(read_file, "rb"))
        document = PDFDocument(parser)
        end = resolve1(document.catalog["Pages"])["Count"]
        text = "ValuepersecurityMarketvalue"

        def func1(i, pdf):
            return pdf.getPage(i).extractText().replace("\n", "")

        pages = [
            i + 1
            for i in range(start, end)
            if text in func1(i, pdf)
            and report.split("-")[-1].strip().upper() in func1(i, pdf)[:85]
        ]
        report = report.split("-")[-1].strip().upper()
    if not pages:
        print(not_found)
        return []
    return tuple(pages)


def parse_type2(pdf, read_file, website, pdf_url, isin):
    """Parse type2 PDF of Roth.

    Keyword Arguments:
    pdf -- PdfFileReader() object
    read_file -- file name
    website -- fund name website
    pdf_url -- PDF link
    """
    _website = website.replace("  ", " ")
    if read_file in file_contents:
        contents = file_contents[read_file]
    else:
        cont_pg = [
            i + 1
            for i in [2, 3, 4]
            if "TABLE OF CONTENTS"
            in pdf.getPage(i).extractText().replace("\n", "")
        ]
        if not cont_pg:
            print("no contents")
            return []
        contents = parse_contents(read_file, cont_pg)
        contents = contents.reset_index(drop=True)
        file_contents[read_file] = contents
    report = re.sub("EdR Fund|EdR Prifund |Alpha ", "", _website).strip()
    pages = find_pages2(read_file, website, contents, pdf, report)
    if not pages:
        return []
    print(pages)
    if "Prifund" in website:
        tables = tabula.read_pdf(
            read_file,
            pages=pages,
            area=[0, 0, 828, 590],
            columns=[127, 370, 390, 450, 505],
            stream=True,
            guess=False,
            silent=True,
        )
        for i in range(len(tables)):
            tables[i].columns = [
                "_1",
                "holding_name",
                "currency",
                "_2",
                "market_value",
                "net_assets",
            ]
            tables[i] = tables[i].drop(columns=["_1", "_2"])
    else:
        tables = tabula.read_pdf(
            read_file,
            pages=pages,
            area=[0, 0, 828, 590],
            columns=[105, 317, 344, 404, 453, 513],
            stream=True,
            guess=False,
            silent=True,
        )
        for i in range(len(tables)):
            tables[i].columns = [
                "_1",
                "holding_name",
                "currency",
                "_2",
                "_3",
                "market_value",
                "net_assets",
            ]
            tables[i] = tables[i].drop(columns=["_1", "_2", "_3"])
    table = pd.concat(tables, ignore_index=True)
    return clean2(table, report, website, pdf_url, isin)


def clean3(table, report, website, pdf_url, isin):
    """Clean dataframe parsed by parse_type3()."""
    table = table[~table["net_assets"].isna()]
    table["fund_name_report"] = report
    table["pdf_url"] = pdf_url
    to_keep = ["Créances", "Dettes", "Comptes financiers"]
    table["currency"] = table["currency"].apply(
        lambda x: re.sub(r"\d+", "", str(x)).strip()
    )
    table = table[
        (
            table["currency"].isin(currencies)
            | table["holding_name"].isin(to_keep)
        )
        & table["net_assets"].notna()
    ]
    table = table.reset_index(drop=True)
    table["market_value"] = table["market_value"].apply(
        lambda x: x.replace(",", ".").replace(" ", "")
    )
    table["net_assets"] = table["net_assets"].apply(
        lambda x: x.replace(",", ".")
    )
    mode = table.currency.mode()[0]
    for i, r in table.iterrows():
        if str(r["currency"]) == "nan":
            table["currency"][i] = mode
    table["fund_name_website"] = website
    table["isin"] = isin
    return table.reset_index(drop=True)


def post_process_contents(contents):
    """Post processing contents from type3 PDF."""
    for idx, r in contents.iterrows():
        if "COMPTES ANNUELS CONSOLIDES AU".lower() in r["fund"]:
            break
    return contents[idx:]


def parse_type3(pdf, read_file, website, pdf_url, isin, recurse_flag):
    """Parse type3 PDF of Roth.

    Keyword Arguments:
    pdf -- PdfFileReader() object
    read_file -- file name
    website -- fund name website
    pdf_url -- PDF link
    recurse_flag -- bool (False in first call,
    True in second call)
    """
    if read_file in file_contents:
        contents = file_contents[read_file]
    else:
        contents = post_process_contents(parse_contents(read_file))
        file_contents[read_file] = contents
    report = website.split("-")[-1].strip()
    start, end = 0, 0
    for idx, r in contents.iterrows():
        if report.lower() in r["fund"].lower():
            start, end = int(r["pg"]), min(
                int(contents["pg"][idx + 1]), pdf.getNumPages()
            )
            break
    if not (start and end) and recurse_flag:
        print(not_found)
        return []

    if not (start and end):
        website = website.replace("Euro", "Europe")
        return parse_type3(pdf, read_file, website, pdf_url, isin, True)

    text = "nominalValeur actuelle"
    website = website.replace("Europe", "Euro") if recurse_flag else website

    def func1(i, pdf):
        return pdf.getPage(i).extractText().replace("\n", "")

    pages = [i + 1 for i in range(start, end) if text in func1(i, pdf)]
    pages = tuple(pages)
    print(pages)
    tables = tabula.read_pdf(
        read_file,
        pages=pages,
        area=[0, 0, 828, 590],
        columns=[305, 345, 423, 501],
        stream=True,
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
        tables[i] = tables[i].drop(columns=["_3"])
    table = pd.concat(tables, ignore_index=True)
    return clean3(table, report, website, pdf_url, isin)


def parse_type4(pdf, read_file, website, pdf_url, isin):
    """Parse type4 PDF of Roth.

    Keyword Arguments:
    pdf -- PdfFileReader() object
    read_file -- file name
    website -- fund name website
    pdf_url -- PDF link
    """
    report = website
    text = "Désignation des valeurs"

    def func1(i, pdf):
        return pdf.getPage(i).extractText().replace("\n", "")

    pages = [
        i + 1 for i in range(15, pdf.getNumPages()) if text in func1(i, pdf)
    ]
    pages = tuple(pages)
    print(pages)
    tables = tabula.read_pdf(
        read_file,
        pages=pages,
        area=[0, 0, 828, 590],
        columns=[325, 351, 420, 500],
        stream=True,
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
        tables[i] = tables[i].drop(columns=["_3"])
    table = pd.concat(tables, ignore_index=True)
    for i, r in table[1:].iterrows():
        if (
            str(r["holding_name"]) == "nan"
            and str(r["currency"]) != "nan"
            and str(r["market_value"]) != "nan"
            and str(r["net_assets"]) != "nan"
        ):
            table["holding_name"][i] = table["holding_name"][i - 1]
    table = table[~table["net_assets"].isna()]
    table["fund_name_report"] = report
    table["pdf_url"] = pdf_url
    to_keep = ["Créances", "Dettes", "Comptes financiers"]
    table["currency"] = table["currency"].apply(
        lambda x: re.sub(r"\d+", "", str(x)).strip()
    )
    table = table[
        (
            table["currency"].isin(currencies)
            | table["holding_name"].isin(to_keep)
        )
        & table["net_assets"].notna()
    ]
    table = table.reset_index(drop=True)
    table["market_value"] = table["market_value"].apply(
        lambda x: x.replace(",", ".").replace(" ", "")
    )
    table["net_assets"] = table["net_assets"].apply(
        lambda x: x.replace(",", ".")
    )
    mode = table.currency.mode()[0]
    for i, r in table.iterrows():
        if str(r["currency"]) == "nan":
            table["currency"][i] = mode
    table["fund_name_website"] = website
    table["isin"] = isin
    return table.reset_index(drop=True)


def wait(path):
    """Wait untill there are no partially downloaded files."""
    while True:
        time.sleep(1)
        if not any(
            ".pdf.crdownload" in x
            for x in [path + "\\" + f for f in os.listdir(path)]
        ):
            break


def download_all(path, pdf_links_df):
    """Download all files in input CSV.

    Keyword Arguments:
    path -- current working directory
    pdf_links_df -- input CSV
    """
    links = set(pdf_links_df["pdf_url"])
    links.discard("annual_report_does_not_exists")
    links_to_pdf_map = {}
    options = webdriver.ChromeOptions()
    options.add_experimental_option(
        "prefs",
        {
            "download.default_directory": path,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "plugins.always_open_pdf_externally": True,
        },
    )
    options.add_argument("--headless")
    driver = webdriver.Chrome(
        ChromeDriverManager().install(), chrome_options=options
    )
    num = 1
    print("Downloading " + str(len(links)) + " files...")
    links = sorted(list(links))
    for link in links:
        if link in ["annual_report_does_not_exists"]:
            continue
        driver.get(link)
        wait(path)
        print(str(num), "file downloaded")
        num += 1
    wait(path)
    files = sorted(
        [path + "/" + f for f in os.listdir(path)], key=os.path.getctime
    )
    files = [x for x in files if ".pdf" in x]
    for i in range(len(links)):
        _ = shutil.move(files[i], "roth" + str(i + 1) + ".pdf")
        links_to_pdf_map[links[i]] = "roth" + str(i + 1) + ".pdf"
    links_to_pdf_map[np.nan] = np.nan
    links_to_pdf_map["annual_report_does_not_exists"] = np.nan
    pdf_links_df["files"] = pdf_links_df["pdf_url"].apply(
        lambda x: links_to_pdf_map[x]
    )
    pdf_links_df.sort_values("files").reset_index(drop=True).to_csv(
        "pdf_names.csv", index=False
    )
    return pdf_links_df


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
    os.chdir(os.path.join(currdir, os.path.normpath(pdf_out_path)))
    if "files" not in pdf_links_df.columns:
        path = os.getcwd()
        pdf_links_df = download_all(path, pdf_links_df)

    tables = []
    for idx, row in (
        pdf_links_df.sort_values("name").reset_index(drop=True).iterrows()
    ):
        website = row["name"]
        read_file = row["files"]
        pdf_url = row["pdf_url"]
        isin = row["isin"]
        if str(read_file) == "nan":
            continue
        pdf = PdfFileReader(read_file)
        pdf._override_encryption = True
        pdf._flatten()
        text = "parsing "
        if (
            "edr fund" in website.lower()
            or "prifund" in website.lower()
            or "visionfund" in website.lower()
        ):
            print(text, read_file, website, end="  ")
            tables.append(parse_type1(pdf, read_file, website, pdf_url, isin))
        elif "edr sicav" in website.lower():
            print(text, read_file, website, end="  ")
            tables.append(
                parse_type3(pdf, read_file, website, pdf_url, isin, False)
            )
        else:
            print(text, read_file, website, end="  ")
            tables.append(parse_type4(pdf, read_file, website, pdf_url, isin))
    _tables = list(filter(lambda x: len(x) > 0, tables))
    result = pd.concat(_tables, ignore_index=True)
    result["currency"] = result["currency"].apply(lambda x: str(x).strip())
    result["holding_name"] = result["holding_name"].apply(
        lambda x: str(x).strip()
    )
    result["market_value"] = result["market_value"].apply(float)
    result["net_assets"] = result["net_assets"].apply(float)
    mode = result.currency.mode()[0]
    for i, r in result.iterrows():
        if str(r["currency"]) == "nan":
            result["currency"][i] = mode
    result["fund_provider"] = "Rothschild Asset Management (FR)"
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
