"""Takes CSV as input. Downloads the PDF files and.

Parses PDFs of provider BNP. Generates a CSV file
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
import time
import datetime

warnings.filterwarnings("ignore")

with open("./src/bnp/currencies.txt") as f:
    currencies = f.readline().split()

done1, type1_cont = {}, {}


def parse_type1(pdf, read_file, website, pdf_url):
    """Parse pdf file 'read_file' for fund name 'website'.

    Keyword Arguments:
    pdf -- PdfFileReader() object
    read_file -- file name
    website -- fund_name_website
    pdf_url -- pdf link
    """
    if "Table des matières" not in extract_text(read_file, page_numbers=[2]):
        return []
        return parse_type3(pdf, read_file, website, pdf_url)
    elif "BNP Paribas" in website:
        return []
        return parse_type2(pdf, read_file, website, pdf_url)
    else:
        if read_file in type1_cont:
            contents = type1_cont[read_file]
        else:
            content_pgs = [
                i + 1
                for i in range(8)
                if "Table des matières"
                in extract_text(read_file, page_numbers=[i])
            ]
            contents = tabula.read_pdf(
                read_file,
                pages=content_pgs,
                area=[0, 0, 828, 590],
                columns=[490],
                guess=False,
                silent=True,
            )
            for i in range(len(contents)):
                contents[i].columns = ["fund", "pg"]
            contents = pd.concat(contents, ignore_index=True)
            contents = contents.dropna()
            contents = contents[contents["pg"].apply(str.isdigit)]
            contents = contents.reset_index(drop=True)
            type1_cont[read_file] = contents
        start, end = 0, 0
        for idx, r in contents.iterrows():
            text = (
                re.sub(
                    "THEAM QUANT- |BNP PARIBAS COMFORT |C CAPITALISATION| LIFE EUR CAPITALISATION|BNP PARIBAS FLEXI I |BNP PARIBAS A FUND ",
                    "",
                    website,
                )
                .split("[")[0]
                .strip()
            )
            if text in r["fund"].upper():
                start, end = (
                    int(r["pg"].strip()),
                    int(contents["pg"][idx + 1].strip()) - 1,
                )
                break
        pages = [
            i + 1
            for i in range(start + 1, end + 2)
            if "Portefeuille-titres au"
            in extract_text(read_file, page_numbers=[i])
        ]
        pages = tuple(pages)
        if not pages:
            print("not found")
            return []
        print(pages)
        if pages in done1:
            table = done1[(pages, read_file)]
        else:
            tables = tabula.read_pdf(
                read_file,
                pages=pages,
                area=[0, 0, 828, 590],
                columns=[100, 352, 373, 498],
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
            done1[(pages, read_file)] = table
        table["fund_name_website"] = website
        table["fund_name_report"] = text.upper()
        table["pdf_url"] = pdf_url
        to_keep = ["CREANCES ET DETTES DIVERSES", "AUTRES"]
        table = table[
            ~table["net_assets"].isna() & table["currency"].isin(currencies)
        ]
        table["holding_name"] = table["holding_name"].apply(
            lambda x: "_" if str(x) == "nan" else x
        )
        table["market_value"] = table["market_value"].apply(
            lambda x: str(x).replace(" ", "").replace("–", "-")
        )
        table = table[
            table["currency"].isin(currencies)
            | table["holding_name"].isin(to_keep)
        ]
        return table.reset_index(drop=True)


type2_cont, done2 = {}, {}


def parse_type2(pdf, read_file, website, pdf_url):
    """Parse pdf file 'read_file' for fund name 'website'.

    Keyword Arguments:
    pdf -- PdfFileReader() object
    read_file -- file name
    website -- fund_name_website
    pdf_url -- pdf link
    """
    if read_file in type2_cont:
        contents = type2_cont[read_file]
    else:
        content_pgs = [
            i + 1
            for i in range(8)
            if "Table des matières"
            in extract_text(read_file, page_numbers=[i])
        ]
        contents = tabula.read_pdf(
            read_file,
            pages=content_pgs,
            area=[0, 0, 828, 590],
            columns=[490],
            guess=False,
            silent=True,
        )
        for i in range(len(contents)):
            contents[i].columns = ["fund", "pg"]
        contents = pd.concat(contents, ignore_index=True)
        contents = contents.dropna()
        contents = contents[contents["pg"].apply(str.isdigit)]
        contents = contents.reset_index(drop=True)
        type2_cont[read_file] = contents
    start, end, report = 0, 0, None
    for idx, r in contents.iterrows():
        text = (
            website.replace("BNP Paribas ", "")
            .replace("Sustainable ", "")
            .replace("  ", " ")
            .split("[")[0]
            .strip()
        )
        if text in r["fund"]:
            start, end = (
                int(r["pg"].strip()),
                int(contents["pg"][idx + 1].strip()) - 1,
            )
            break
    pages = [
        i + 1
        for i in range(start + 1, end + 2)
        if "Portefeuille-titres au"
        in extract_text(read_file, page_numbers=[i])
    ]
    pages = tuple(pages)
    print(pages)
    if pages in done2:
        table = done2[(pages, read_file)]
    else:
        tables = tabula.read_pdf(
            read_file,
            pages=pages,
            area=[0, 0, 828, 590],
            columns=[71, 186, 204, 263, 299, 338, 456, 473, 533],
            guess=False,
            silent=True,
        )
        for i in range(len(tables)):
            df1, df2 = tables[i].iloc[:, :5], tables[i].iloc[:, 5:]
            if len(df1.columns) == 5 and len(df2.columns) == 5:
                df1.columns = df2.columns = [
                    "_1",
                    "holding_name",
                    "currency",
                    "market_value",
                    "net_assets",
                ]
                tables[i] = pd.concat([df1, df2], ignore_index=True)
                tables[i].drop(columns=["_1"], inplace=True)
            elif len(df1.columns) == 5:
                df1.columns = [
                    "_1",
                    "holding_name",
                    "currency",
                    "market_value",
                    "net_assets",
                ]
                tables[i] = df1
                tables[i].drop(columns=["_1"], inplace=True)
            elif len(df2.columns) == 5:
                df2.columns = [
                    "_1",
                    "holding_name",
                    "currency",
                    "market_value",
                    "net_assets",
                ]
                tables[i] = df2
                tables[i].drop(columns=["_1"], inplace=True)
        table = pd.concat(tables, ignore_index=True)
        done2[(pages, read_file)] = table

    table["fund_name_website"] = website
    table["fund_name_report"] = text
    table["pdf_url"] = pdf_url
    cond1 = (
        table["holding_name"].isna()
        & table["currency"].notna()
        & table["market_value"].notna()
        & table["net_assets"].notna()
    )
    cond2 = (
        table["holding_name"].notna()
        & table["currency"].isna()
        & table["market_value"].isna()
        & table["net_assets"].isna()
    )
    cond3 = table.notna().all(axis=1)
    for x in table.iloc[:, 1:4].isna().all(axis=1).index:
        if x >= len(table) - 1:
            break
        i, s = x + 1, table.iloc[x, 0] if type(table.iloc[x, 0]) == str else ""
        while table.iloc[i, 1:4].isna().all() and i < len(table) - 1:
            s = (
                s + " " + table.iloc[i, 0]
                if type(table.iloc[i, 0]) == str
                else s
            )
            i += 1
        s = s + " " + table.iloc[i, 0] if type(table.iloc[i, 0]) == str else s
        currency = table.iloc[i, 1]
        value = table.iloc[i, 2]
        net_assets = table.iloc[i, 3]
        table.iloc[x, :4] = [s, currency, value, net_assets]
    table = (
        table.groupby(["currency", "market_value", "net_assets"])
        .tail(1)
        .reset_index(drop=True)
    )
    table = table[table["currency"].isin(currencies)]
    table["market_value"] = table["market_value"].apply(
        lambda x: str(x).replace(" ", "").replace("–", "-")
    )
    return table.reset_index(drop=True)


done3 = {}

type3_cont = {}


def parse_type3(pdf, read_file, website, pdf_url):
    """Parse pdf file 'read_file' for fund name 'website'.

    Keyword Arguments:
    pdf -- PdfFileReader() object
    read_file -- file name
    website -- fund_name_website
    pdf_url -- pdf link
    """
    typ = None
    if "CRELAN PENSION FUND" in website:
        pages = []
        for i in range(pdf.getNumPages() - 2):
            text = extract_text(read_file, page_numbers=[i]).replace("\n", "")
            k = i
            if (
                website.split("[")[0].strip() in text
                and "Dénomination Quantité au" in text
                and "Devise Cours en" in text
            ):
                while (
                    website.split("[")[0].strip() in text
                    and "Dénomination Quantité au" in text
                    and "Devise Cours en" in text
                ):
                    pages.append(k + 1)
                    k += 1
                    text = extract_text(read_file, page_numbers=[k]).replace(
                        "\n", ""
                    )
                pages = tuple(pages)
                break
    else:
        if read_file in type3_cont:
            contents = type3_cont[read_file]
        else:
            contents = tabula.read_pdf(
                read_file,
                pages=[2, 3, 4, 5],
                area=[0, 50, 835, 590],
                columns=[545],
                guess=False,
                silent=True,
            )
            start, end = 0, 0
            contents = [x for x in contents if x.shape[1] == 2]
            for i in range(len(contents)):
                contents[i].columns = ["fund", "pg"]
            type3_cont[read_file] = contents
        if contents:
            contents = pd.concat(contents, ignore_index=True)
            contents = contents.dropna()
            contents = contents[
                contents["pg"].apply(lambda x: type(x) == float)
            ]
            contents = contents.reset_index(drop=True)
            for idx, r in contents[:].iterrows():
                found = False
                if "Composition des actifs au" in r["fund"]:
                    if idx == len(contents) - 1:
                        start, end = int(contents["pg"][idx]), min(
                            int(contents["pg"][idx]) + 6, pdf.getNumPages()
                        )
                    else:
                        start, end = int(r["pg"]), int(contents["pg"][idx + 1])
                    break
                text = (
                    re.sub(
                        r"BNP PARIBAS B STRATEGY |METROPOLITAN-RENTASTRO |BNPPF PRIVATE |BNPPF S-FUND |BPOST BANK FUND |BNP PARIBAS B INVEST |BNP PARIBAS B CONTROL |BNP PARIBAS A FUND ",
                        "",
                        website,
                    )
                    .split("[")[0]
                    .lower()
                    .strip()
                )
                if text in r["fund"].lower():
                    for i in range(idx, len(contents)):
                        if (
                            "COMPOSITION DES ACTIFS ET CHIFFRES"
                            in contents["fund"][i]
                        ):
                            if i == len(contents) - 1:
                                start, end = int(contents["pg"][i]), min(
                                    int(contents["pg"][i]) + 6,
                                    pdf.getNumPages(),
                                )
                            else:
                                start, end = int(contents["pg"][i]), int(
                                    contents["pg"][i + 1]
                                )
                                found = True
                                break
                    if found:
                        break
            if not (start and end):
                print("not found ")
                return []
            pages = tuple(range(start, end))
        else:
            typ = 1
            for i in range(pdf.getNumPages() - 1, 0, -1):
                if (
                    "Inventaire des instruments financiers au"
                    in pdf.getPage(i).extractText()
                ):
                    page = i + 1
                    break
            if not page:
                print("not found")
                return []
            pages = tuple(range(page, pdf.getNumPages() + 1))
    print(pages)
    if (read_file, pages) in done3:
        table = done3[read_file, pages]
    else:
        if typ:
            tables = tabula.read_pdf(
                read_file,
                pages=pages,
                area=[0, 0, 835, 590],
                columns=[254, 319, 373, 405, 506],
                guess=False,
                silent=True,
            )
        else:
            tables = tabula.read_pdf(
                read_file,
                pages=pages,
                area=[0, 0, 835, 590],
                lattice=True,
                #columns=[251, 323, 356, 413, 463, 536],
                guess=False,
                silent=True,
            )
        _tables = []
        for i in range(len(tables)):
            if len(tables[i].columns) == 8:
                tables[i].columns = [
                    "holding_name",
                    "_1",
                    "currency",
                    "_2",
                    "market_value",
                    "_3",
                    "_4",
                    "net_assets",
                ]
                tables[i] = tables[i].drop(columns=["_1", "_2", "_3","_4"])
                _tables.append(tables[i])
            if len(tables[i].columns) == 7:
                tables[i].columns = [
                    "holding_name",
                    "_1",
                    "currency",
                    "_4"
                    "market_value",
                    "_3"
                    "net_assets",
                ]
                tables[i] = tables[i].drop(columns=["_1", "_2","_3"])
            df = tables[i].copy()
            
                _tables.append(tables[i])
        table = pd.concat(_tables, ignore_index=True)
        done3[(read_file, pages)] = table
    table["fund_name_report"] = " ".join(website.split()[:-1])
    table["fund_name_website"] = website
    table["pdf_url"] = pdf_url
    to_keep = ["CREANCES ET DETTES DIVERSES", "AUTRES"]
    table = table[
        (
            table["currency"].isin(currencies)
            | table["holding_name"].isin(to_keep)
        )
        & table["market_value"].notna()
    ]
    if typ:
        table["market_value"] = table["market_value"].apply(
            lambda x: str(x).replace(",", "").replace("–", "-")
        )
    else:
        table["market_value"] = table["market_value"].apply(
            lambda x: str(x)
            .replace(".", "")
            .replace(",", ".")
            .replace("–", "-")
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
            links_to_pdf_map[link] = "BNP_" + str(num) + ".pdf"
            with open("BNP_" + str(num) + ".pdf", "wb") as f:
                print("BNP_" + str(num) + ".pdf")
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
    start = time.time()
    for idx, row in pdf_links_df.iterrows():
        table = []
        website = row["name"]
        read_file = row["files"]
        pdf_url = row["pdf_url"]
        isin = row["isin"]
        if str(read_file) == "nan":
            issues.append([website, read_file])
            continue
        try:
            pdf = PdfFileReader(read_file)
        except BaseException:
            print("Error reading PDF", read_file, "containing fund: ", website)
            continue
        pdf._override_encryption = True
        pdf._flatten()
        print("parsing ", read_file, website, end="   ")
        table = parse_type1(pdf, read_file, website, pdf_url)
        if len(table) > 0:
            table["isin"] = isin
            tables.append(table)

    print("Time elapsed: ", datetime.timedelta(seconds=time.time() - start))

    tables = list(filter(lambda x: type(x) != str and len(tables) > 0, tables))
    result = pd.concat(tables, ignore_index=True)
    result["currency"] = result["currency"].apply(lambda x: str(x).strip())
    result["holding_name"] = result["holding_name"].apply(
        lambda x: str(x).strip()
    )
    result = result[~result["net_assets"].isna()]
    result = result.reset_index(drop=True)
    result["market_value"] = result["market_value"].apply(
        lambda x: "0.0" if x == "-" else x
    )
    result["net_assets"] = result["net_assets"].apply(
        lambda x: "0.0" if x == "-" else x
    )
    result["market_value"] = result["market_value"].apply(
        lambda x: float(
            "-"
            + re.sub(r"[)(]", "", x)
        )
        if re.search(r"[)(]", x)
        else float(x)
    )
    result["net_assets"] = result["net_assets"].apply(
        lambda x: float(
            "-"
            + re.sub(r"[)(]", "", str(x)).replace("%", "").replace(",", ".")
        )
        if re.search(r"[)(]", str(x))
        else float(str(x).replace("%", "").replace(",", ".").replace(" ", ""))
    )

    for i, r in result.iterrows():
        if str(r["currency"]) == "nan":
            result["currency"][i] = result["currency"][i - 1]
    result["holding_name"] = result["holding_name"].apply(
        lambda x: "_" if x == "nan" else x
    )
    result["fund_provider"] = "BNP Paribas Asset Management (BE)"
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
