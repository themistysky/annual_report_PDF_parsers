"""Parse all types of PDF files of financial reports of Aviva company.

Module functions:
    parse_type1_pdf(args): --> pandas.DataFrame
    parse_type2_pdf(args): --> pandas.DataFrame
    parse_type1a_table(read_file, start, end,
    fund_name, fund_name_website, isin): -->pandas.DataFrame
"""

import re
import sys
import os
import warnings
from pdfminer.high_level import extract_text
from PyPDF2 import PdfFileReader
import tabula
from country_list import countries_for_language as countries_in
import pandas as pd
import numpy as np
import requests


COUNTRIES = list(countries_in("en"))
COUNTRIES = [x[1] for x in COUNTRIES]
COUNTRIES.extend(
    [
        "United States of America",
        "Virgin Islands, British",
        "Virgin Islands, US",
        "Virgin Islands, U.S.",
        "Convertible Bonds",
    ]
)

COLUMN_NAMES = ["holding_name", "market_value", "currency", "net_assets"]
warnings.filterwarnings("ignore")

# Two types of PDFs in Aviva have been encountered so far.
# Type 1 PDF is in English , Type 2 PDF is in French
# parse_type1_pdf() parses the first type and parse_type2_pdf()
# parses the second type.
# There are two types of tables in the first type of PDF.
# parse_type1_pdf() parses the first type and parse_type1a_table()
# parses the second.


def parse_type1_pdf(args):
    """Takes result DataFrame and returns after appending it with DataFrame.

    created by parsing type 1 PDF file read_file
    for fund name fund_name.
    Input: list(args)
    Output: pandas.DataFrame
    """
    (
        result,
        read_file,
        fund_name,
        fund_name_website,
        isin,
        currencies,
        pdf_url,
    ) = args
    print("Parsing:", read_file, " for ", fund_name + "\n")

    countries = COUNTRIES[:]
    pdf = PdfFileReader(read_file)
    total_pages = pdf.getNumPages()

    pages = [
        i + 1
        for i in range(11, total_pages)
        if fund_name.upper()
        in pdf.getPage(i).extractText().replace(" - ", " ")
    ]

    if len(pages) == 0:
        contents = tabula.read_pdf(
            read_file, pages=[2], stream=True, guess=False, silent=True
        )[0]
        start, end = 0, 0

        for i in range(len(contents)):
            if fund_name in contents[contents.columns[0]][i]:
                start = int(contents.iloc[i, 0].split()[-1])
                end = (
                    min(int(contents.iloc[i + 1, 0].split()[-1]), total_pages)
                    - 1
                )
                break

        if start and end:
            result = pd.concat(
                [
                    result,
                    parse_type1a_tables(
                        read_file,
                        start,
                        end,
                        fund_name,
                        fund_name_website,
                        isin,
                        pdf_url,
                    ),
                ],
                ignore_index=True,
            )
            return result

        del start, end
        return result

    # Parsing table type 1 in PDF type 1

    fund_name = fund_name.split()
    fund_name.insert(2, "-")
    fund_name = " ".join(fund_name)

    # Each page contain two tables, each are extracted separately.
    left_tables = tabula.read_pdf(
        read_file,
        pages=pages[0],
        area=(210, 35, 790, 300),
        stream=True,
        guess=False,
        silent=True,
    )

    left_tables = left_tables + tabula.read_pdf(
        read_file,
        pages=pages[1:],
        area=(175, 35, 790, 300),
        stream=True,
        guess=False,
        silent=True,
    )

    right_tables = tabula.read_pdf(
        read_file,
        pages=pages,
        area=(175, 302, 790, 560),
        stream=True,
        guess=False,
        silent=True,
    )
    # Correcting table format if incorrect format detected
    left_tables = correct_corrupt_tables(left_tables, currencies)
    right_tables = correct_corrupt_tables(right_tables, currencies)

    # Removing unwanted rows containg country names
    remove_country_rows(countries, left_tables)
    remove_country_rows(countries, right_tables)

    # all_tables = left_tables + right_tables
    if len(left_tables) > 0 and len(right_tables) > 0:
        all_tables = pd.concat(
            [pd.concat(left_tables), pd.concat(right_tables)],
            ignore_index=True,
        )
    elif len(left_tables) == 0:
        all_tables = pd.concat(right_tables)
    elif len(right_tables) == 0:
        all_tables = pd.concat(left_tables)
    else:
        return result

    # Cleanig the table
    for index, row in all_tables.iterrows():
        condition_1 = pd.isnull(row["currency"]) and (
            not pd.isnull(row["market_value"])
            or not pd.isnull(row["net_assets"])
        )
        condition_2 = (
            "Total" in str(row["holding_name"])
            or "transferable securities" in str(row["holding_name"]).casefold()
            or "–" in str(row["holding_name"])
            or "–" in str(row["market_value"])
        )
        if condition_1 or condition_2:
            all_tables.drop(index, inplace=True)
    all_tables = all_tables[~all_tables["holding_name"].isnull()]
    all_tables.reset_index(drop=True, inplace=True)
    last = all_tables["currency"][0]
    name = ""
    for index, row in all_tables.iterrows():
        name = name + " " + row["holding_name"]
        if not row.isna().any():
            row["holding_name"] = name
            row["fund_name_report"] = fund_name
            row["fund_name_website"] = fund_name_website
            row["isin"] = isin

            if row["currency"] not in currencies:
                row["currency"] = last
            else:
                last = row["currency"]

            result = result.append(row, ignore_index=True)
            name = ""
    result["market_value"] = result["market_value"].apply(
        lambda x: str(x).split()[-1].replace(",", "")
    )
    result = result[
        (
            result["market_value"].apply(
                lambda x: str(x).replace(".", "").isdigit()
            )
        )
        & (
            result["net_assets"].apply(
                lambda x: str(x).replace(".", "").isdigit()
            )
        )
    ]
    result["pdf_url"] = pdf_url
    return result


def correct_corrupt_tables(tables, currencies):
    """Corrects tables read with incorrect format.

    Returns list of tables with all tables with columns =
    "holding_name","currency",market_value","net_assets"
    Input: list(args)
    Output: pandas.DataFrame
    """
    tables = [x for x in tables if x.shape[1] in [5, 4]]

    for i, table in enumerate(tables):
        if table.isna().all().any():
            table.dropna(axis=1, how="all", inplace=True)
        if table.shape[1] < 4:
            table = pd.DataFrame(
                columns=[
                    "holding_name",
                    "currency",
                    "_",
                    "market_value",
                    "net_assets",
                ]
            )

        if table.shape[1] == 4:
            table.insert(loc=1, column="currency", value=np.nan)

        tables[i] = table

    for i, table in enumerate(tables):
        table.columns = [
            "holding_name",
            "currency",
            "_",
            "market_value",
            "net_assets",
        ]

        table.reset_index(drop=True)

        for idx, row in table.iterrows():

            for currency in currencies:

                if currency in str(row["holding_name"]):
                    table["holding_name"][idx] = row["holding_name"].replace(
                        currency, ""
                    )
                    table["currency"][idx] = currency
                    break

        table.drop(columns=["_"], inplace=True)
        tables[i] = table

    return tables


def remove_country_rows(countries, tables):
    """Removes unwanted rows containing country names."""
    for table in tables:
        _countries = countries[:]

        for index, row in table.iterrows():
            if row["holding_name"] in _countries:
                table.drop(index, inplace=True)
                _countries.remove(row["holding_name"])


# Parses second type of table in first type of PDF
def parse_type1a_tables(
    read_file, start, end, fund_name, fund_name_website, isin, pdf_url
):
    """Takes in file name containg second type of table in type 1 PDF.

    Returns DataFrame with column names set as COLUMN_NAMES.
    """
    tables = tabula.read_pdf(
        read_file,
        pages=list(range(int(start), end + 1)),
        stream=True,
        guess=False,
        silent=True,
    )

    for table in tables[:]:
        if table.shape[1] == 6:

            table[table.columns[0]] = table[table.columns[0]].astype(
                str
            ) + table[table.columns[1]].astype(str)
            table.drop(
                columns=[table.columns[1], table.columns[2]], inplace=True
            )
            table.columns = COLUMN_NAMES
            table["fund_name_report"] = fund_name
            table["fund_name_website"] = fund_name_website
            table["isin"] = isin

        elif table.shape[1] == 5:

            table.drop(columns=[table.columns[2]], inplace=True)
            table.columns = COLUMN_NAMES
            table["fund_name_report"] = fund_name
            table["fund_name_website"] = fund_name_website
            table["isin"] = isin

        elif table.shape[1] == 7:
            col1 = table[table.columns[0]].astype(str)
            col2 = table[table.columns[1]].astype(str)
            col3 = table[table.columns[2]].astype(str)
            table[table.columns[0]] = col1 + col2 + col3
            table.drop(
                columns=[
                    table.columns[1],
                    table.columns[2],
                    table.columns[3],
                ],
            )
            table.columns = COLUMN_NAMES
            table["fund_name_report"] = fund_name
            table["fund_name_website"] = fund_name_website
            table["isin"] = isin

        else:

            tables.remove(table)

    result = pd.concat(tables, ignore_index=True)
    result["pdf_url"] = pdf_url
    return result


def parse_type2_pdf(args):
    """Takes 'result' DataFrame and returns after appending it with.

    DataFrame created by parsing type 2 PDF 'read_file'
    for fund name 'fund_name'
    """
    (
        result,
        read_file,
        fund_name,
        fund_name_website,
        isin,
        currencies,
        pdf_url,
    ) = args
    print("Parsing:", read_file, " for ", fund_name + "\n")
    pdf = PdfFileReader(read_file)
    total_pages = pdf.getNumPages()

    for i in range(total_pages, 0, -1):
        text = extract_text(read_file, page_numbers=[i], codec="iso-8859-1")
        cond_1 = "inventaire" in text
        cond_2 = "Libellé valeur" in text
        cond_3 = "Valeur \nboursière" in text and "% Actif\nnet" in text
        if cond_1 and cond_2 and cond_3:
            start_page = i
            break

    tables = tabula.read_pdf(
        read_file,
        pages=[start_page + 1],
        area=(185, 41, 810, 558),
        stream=True,
        guess=False,
        silent=True,
    )

    tables = tables + tabula.read_pdf(
        read_file,
        pages=list(range(start_page + 2, total_pages + 1)),
        area=(80, 41, 810, 558),
        stream=True,
        guess=False,
        silent=True,
    )

    tables = [table for table in tables if table.shape[1] in [5, 6, 7]]

    for table in tables:

        if table.shape[1] == 5:
            parse_five_columns(table)

        elif table.shape[1] == 6:
            parse_six_columns(table)

        else:
            parse_seven_columns(table)

    table = pd.concat(tables, ignore_index=True)
    cond_1 = table["holding_name"] == "Total Obligation"
    cond_2 = table["holding_name"] == "Total Action"
    cond_3 = table["holding_name"] == "Total O.P.C.V.M."
    #table = table[: table.index[(cond_1) | (cond_2) | (cond_3)].to_list()[0]]

    table = table[table["currency"].isin(currencies)]

    table["fund_name_report"] = fund_name
    table["fund_name_website"] = fund_name_website
    table["isin"] = isin

    result = pd.concat([result, table], ignore_index=True)
    result["market_value"] = result["market_value"].apply(
        lambda x: str(x).replace(",", ".").replace(" ", "")
    )
    result["net_assets"] = result["net_assets"].apply(
        lambda x: str(x).replace(",", ".")
    )
    result = result[
        (
            result["market_value"].apply(
                lambda x: str(x).replace(".", "").isdigit()
            )
        )
        & (
            result["net_assets"].apply(
                lambda x: str(x).replace(".", "").isdigit()
            )
        )
    ]
    result["pdf_url"] = pdf_url
    return result


def parse_five_columns(table):
    """Cleans tables containing 5 columns from Type 2 PDF.

    Input: pandas.DataFrame
    Output: pandas.DataFrame
    """
    table.drop(table.columns[[1]], axis=1, inplace=True)
    table.columns = COLUMN_NAMES

    for index, row in table.iterrows():

        table["holding_name"][index] = re.sub(
            r"[A-Z]{2}\d{10}|[A-Z]{2}[A-Z0-9]{9}\d|PROPRE",
            "",
            str(row["holding_name"]),
        )

        if pd.isna(row["net_assets"]):
            match = re.search(r"\d+,\d{2}", str(row["holding_name"]))

            if match:
                table["net_assets"][index] = match.group()

    table.reset_index(drop=True, inplace=True)


def parse_six_columns(table):
    """Cleans tables containing 6 columns from Type 2 PDF.

    Input: pandas.DataFrame
    Output: pandas.DataFrame
    """
    table.drop(columns=[table.columns[1], table.columns[2]], inplace=True)
    table.columns = COLUMN_NAMES

    for index, row in table.iterrows():
        table["holding_name"][index] = re.sub(
            r"[A-Z]{2}\d{10}|[A-Z]{2}[A-Z0-9]{9}\d",
            "",
            str(row["holding_name"]),
        )

        if pd.isna(row[-1]):
            match = re.search(r"\d+,\d{2}", str(row[0]))

            if match:
                table[table.columns[-1]][index] = match.group()

    table.reset_index(drop=True, inplace=True)


def parse_seven_columns(table):
    """Cleans tables containing 7 columns from Type 2 PDF.

    Input: pandas.DataFrame
    Output: pandas.DataFrame
    """
    if table.isna().all().any():
        table.drop(
            columns=[table.columns[1], table.columns[2], table.columns[3]],
            inplace=True,
        )

    else:
        table.drop(
            columns=[table.columns[0], table.columns[2], table.columns[3]],
            inplace=True,
        )

    table.columns = COLUMN_NAMES

    for index, row in table.iterrows():
        table["holding_name"][index] = re.sub(
            r"[A-Z]{2}\d{10}|[A-Z]{2}[A-Z0-9]{9}\d",
            "",
            str(row["holding_name"]),
        )

        if pd.isna(row[-1]):
            match = re.search("\\d+,\\d{2}", str(row[0]))

            if match:
                table[table.columns[-1]][index] = match.group()

    table.reset_index(drop=True, inplace=True)


def main():
    """Main.

    Loops through the input CSV and calls relevant parsing functions
    based on PDF types.
    """
    assert "-p" in sys.argv, "No pdf download path provided as argument."
    assert "-i" in sys.argv, "No input file path provided as argument."
    assert "-o" in sys.argv, "No output file path provided as argument."
    currdir = os.getcwd()
    in_path = sys.argv[sys.argv.index("-i") + 1]
    out_path = sys.argv[sys.argv.index("-o") + 1]
    pdf_out_path = sys.argv[sys.argv.index("-p") + 1]
    with open(
        os.path.join(currdir, r"src/aviva/currencies.txt"), encoding="utf-8"
    ) as f:
        currencies = f.readline().split()
    print("Taking input from " + in_path)
    pdf_links_df = pd.DataFrame(pd.read_csv(os.path.join(currdir, in_path)))
    os.chdir(os.path.join(currdir, pdf_out_path))
    if len(pdf_links_df.columns) == 3:
        links_set = set(pdf_links_df["pdf_url"])

        links_to_pdf_map = {}

        num = 1
        links_set.discard("annual_report_does_not_exists")
        links_set.discard("nan")
        print("Downloading " + str(len(links_set)) + " files...")
        for link in links_set:
            if link in ["nan", "annual_report_does_not_exists"]:
                links_to_pdf_map[link] = np.nan
                continue

            response = requests.get(link)

            links_to_pdf_map[link] = "Aviva" + str(num) + ".pdf"
            print("Aviva" + str(num) + ".pdf")
            with open("Aviva" + str(num) + ".pdf", "wb") as file:
                file.write(response.content)
            num += 1

        print(len(links_set), " files downloaded")
        links_to_pdf_map["annual_report_does_not_exists"] = np.nan
        links_to_pdf_map["nan"] = np.nan

        pdf_links_df["files"] = pdf_links_df["pdf_url"].apply(
            lambda x: links_to_pdf_map[x]
        )
        del num, links_set
    pdf_links_df.dropna(inplace=True)
    pdf_links_df.to_csv("aviva_pdf_names.csv", index=False, mode="w")

    result = pd.DataFrame(
        columns=[
            "fund_name_report",
            "fund_name_website",
            "isin",
            "holding_name",
            "market_value",
            "currency",
            "net_assets",
            "pdf_url",
        ]
    )

    done1, done2, type1, type2 = [], [], [], []

    for row in pdf_links_df.iterrows():
        row = row[1]
        pdf_url = row["pdf_url"]
        fund_name_website = row["name"]
        isin = row["isin"]
        read_file = row["files"]

        if "Fund" in row["name"]:
            fund_name = " ".join(
                row["name"].split()[: row["name"].split().index("Fund") + 1]
            )

            if not pd.isna(read_file) and fund_name not in done1:
                type1.append(
                    [
                        result,
                        read_file,
                        fund_name,
                        fund_name_website,
                        isin,
                        currencies[:],
                        pdf_url,
                    ]
                )
                done1.append(fund_name)

        elif (not pd.isna(row["files"])) and read_file not in done2:
            cond1 = "-" in row["name"]
            if cond1:
                val = row["name"].split()[: row["name"].split().index("-")]
                fund_name = " ".join(val)
            else:
                fund_name = row["name"]

            type2.append(
                [
                    result,
                    read_file,
                    fund_name,
                    fund_name_website,
                    isin,
                    currencies[:],
                    pdf_url,
                ]
            )
            done2.append(read_file)

        else:
            pass
    type2result = list(map(parse_type2_pdf, type2))
    type1result = list(map(parse_type1_pdf, type1))

    result = pd.concat(type2result + type1result, ignore_index=True)
    result = result[result["currency"].isin(currencies)]
    result = result[
        result["net_assets"].apply(lambda x: bool(re.search(r"\d", str(x))))
    ]
    result = result[
        ~(
            result["holding_name"].apply(
                lambda x: bool(re.search(r"^\s+$|.{90}", x))
            )
        )
    ]
    result["market_value"] = result["market_value"].apply(
        lambda x: str(x)[::-1]
        .replace(",", ".", 1)[::-1]
        .replace(",", "")
        .replace(" ", "")
    )
    result["net_assets"] = result["net_assets"].apply(
        lambda x: str(x).replace(",", ".")
    )
    for idx, row in result.iterrows():
        result["holding_name"][idx] = re.sub(
            r"[^0-9a-zA-Z\/ %)(]", "", row["holding_name"]
        )
        if row["holding_name"].count("%") > 1:
            text = result["holding_name"][idx]
            s = re.search(r"% {0,3}[\d/]+", text)
            result["holding_name"][idx] = text[: s.end()]

        result["holding_name"][idx] = row["holding_name"].strip()
    result = result[
        (result["market_value"].apply(lambda x: x.replace(".", "").isdigit()))
        & (result["net_assets"].apply(lambda x: x.replace(".", "").isdigit()))
    ]
    result["market_value"] = result["market_value"].apply(lambda x: float(x))
    result["net_assets"] = result["net_assets"].apply(lambda x: float(x))
    result.insert(loc=0, column="fund_provider", value="Aviva")

    result.to_csv(
        os.path.join(currdir, out_path),
        index=False,
        mode="w",
    )
    print("All Funds Parsed.\n")
    print("Total - ", len(result))


if __name__ == "__main__":
    main()
