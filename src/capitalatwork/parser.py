"""Parse all types of PDFs of financial reports of Capitalatwork.

Module functions:
    manage_tables(args : list): --> pd.DataFrame
    parse_four_columns(table: pd.DataFrame, currencies: str): --> pd.DataFrame
    parse_five_columns(table: pd.DataFrame): --> pd.DataFrame
    parse_six_columns(table: pd.DataFrame): --> pd.DataFrame
"""
import warnings
import re
import os
import sys
from pdfminer.high_level import extract_text
import tabula
from country_list import countries_for_language as countries_in
import pandas as pd
from pandas import DataFrame as DF
import numpy as np
import requests

warnings.filterwarnings("ignore")

COUNTRIES = list(countries_in("en"))
COUNTRIES = [x[1] for x in COUNTRIES]
# Rows containing these values shall be removed.
COUNTRIES.extend(
    [
        "United States of America",
        "Virgin Islands, British",
        "Virgin Islands, US",
        "Virgin Islands, U.S.",
        "Convertible Bonds",
        "Equities",
        "Bonds",
        "Options",
        "Financial instruments",
        "Investment funds (UCITS)",
        "Investment funds",
        "Long Positions",
    ]
)


def manage_tables(args):
    """Takes a dataframe and resturns the result after passing.

    it to relevant parser
    Input: list(table, currdir)
    table: pandas.DataFrame
    currdir: str
    Output: pandas.DataFrame
    """
    table, currdir = args
    with open(
        os.path.join(currdir, r"src/capitalatwork/currencies.txt"),
    ) as f:
        currencies = f.readline().split()
    if table.shape[1] == 4:
        return parse_four_columns(table, currencies)
    if table.shape[1] == 5:
        return parse_five_columns(table, currencies)
    if table.shape[1] == 6:
        return parse_six_columns(table, currencies)
    return DF(
        columns=[
            "holding_name",
            "currency",
            "market_value",
            "net_assets",
        ]
    )


def parse_four_columns(table, currencies):
    """Takes a dataframe with 4 columns and returns a formatted version.

    Input: table: pandas.DataFrame
    currencies: list
    Output: pandas.DataFrame
    """
    table.columns = [
        "holding_name",
        "acquisition",
        "market_value",
        "net_assets",
    ]
    table.insert(loc=1, column="currency", value=np.nan)

    for index, row in table.iterrows():
        for currency in currencies:
            if " " + currency in " * ".join(map(str, row)):
                table["holding_name"][index] = re.sub(
                    "\\d*$", "", str(row["holding_name"]).replace(",", "")
                ).replace(" " + currency, " ")
                table["currency"][index] = currency
                break
    table = table.drop(columns=["acquisition"])
    return table


def parse_five_columns(table, currencies):
    """Takes a dataframe with 5 columns and returns a formatted version.

    Input: pandas.DataFrame
    Output: pandas.DataFrame
    """
    table.columns = [
        "holding_name",
        "currency",
        "acquisition",
        "market_value",
        "net_assets",
    ]
    for index, row in table.iterrows():
        table["holding_name"][index] = re.sub(
            "\\d*$", "", str(row["holding_name"]).replace(",", "")
        )
        for currency in currencies:
            if " " + currency in " * ".join(map(str, row)):
                table["holding_name"][index] = str(
                    row["holding_name"]
                ).replace(" " + currency, " ")
                table["currency"][index] = currency
                break
    table = table.drop(columns=["acquisition"])
    return table


def parse_six_columns(table, currencies):
    """Takes a dataframe with 6 columns and returns a formatted version.

    Input: pandas.DataFrame
    Output: pandas.DataFrame
    """
    table.columns = [
        "holding_name",
        "quantity",
        "currency",
        "acquisition",
        "market_value",
        "net_assets",
    ]
    for index, row in table.iterrows():
        table["holding_name"][index] = re.sub(
            "\\d*$", "", str(row["holding_name"]).replace(",", "")
        )
        for currency in currencies:
            if " " + currency in " * ".join(map(str, row)):
                table["holding_name"][index] = str(
                    row["holding_name"]
                ).replace(" " + currency, " ")
                table["currency"][index] = currency
                break
    table = table.drop(columns=["acquisition", "quantity"])
    return table


def main():
    """Main.

    Reads input CSV file, extracts table from
    PDFs passes them to manage_tables()
    """
    assert "-p" in sys.argv, "No pdf download path provided as argument."
    assert "-i" in sys.argv, "No input file path provided as argument."
    assert "-o" in sys.argv, "No output file path provided as argument."

    currdir = os.getcwd()
    in_path = sys.argv[sys.argv.index("-i") + 1]
    out_path = sys.argv[sys.argv.index("-o") + 1]
    pdf_out_path = sys.argv[sys.argv.index("-p") + 1]
    print("Taking input from " + in_path)
    pdf_links_df = pd.DataFrame(pd.read_csv(os.path.join(currdir, in_path)))
    links_set = set(pdf_links_df["pdf_url"])
    links_set.discard("annual_report_does_not_exists")
    os.chdir(os.path.join(currdir, pdf_out_path))
    if len(pdf_links_df.columns) == 2:
        links_set = set(pdf_links_df["pdf_url"])
        links_set.discard("nan")
        links_set.discard("annual_report_does_not_exists")
        links_to_pdf_map = {}

        num = 1

        print("Downloading " + str(len(links_set)) + " files...")
        for link in links_set:
            if link in ["nan", "annual_report_does_not_exists"]:
                links_to_pdf_map[link] = np.nan
                continue

            response = requests.get(link)
            links_to_pdf_map[link] = "capitalatwork_" + str(num) + ".pdf"
            print("capitalatwork_" + str(num) + ".pdf")
            with open("capitalatwork_" + str(num) + ".pdf", "wb") as file:
                file.write(response.content)
            num += 1

        links_set.discard("annual_report_does_not_exists")

        pdf_links_df["files"] = pdf_links_df["pdf_url"].apply(
            lambda x: links_to_pdf_map[x]
        )

        del num

        pdf_links_df.to_csv(
            "capitalatwork_pdf_names.csv",
            mode="w",
            index=False,
        )

    fund_to_tables = {}
    if len(links_set) == 1:
        read_file = pdf_links_df["files"][0]
        contents_list = extract_text(
            read_file,
            page_numbers=[1],
        ).split("\n")

    else:
        read_file, contents_list = None, None

    for index, row in pdf_links_df.iterrows():
        fund_name_website = row["name"]
        fund_name = row["name"].split(" - ")[0]
        pdf_url = row["pdf_url"]
        if not read_file:
            read_file = row["files"]
            contents_list = extract_text(read_file, page_numbers=[1]).split(
                "\n"
            )

        start, end = 0, 0
        for i in range(len(contents_list)):
            if " ".join(fund_name.split()) in contents_list[i]:
                start = int(contents_list[i].split()[-1])
                end = int(contents_list[i + 1].split()[-1]) - 1
                break

        if start != 0 and end != 0:
            print("Parsing : " + read_file + " for " + fund_name + "\n")
            tables = tabula.read_pdf(
                read_file,
                pages=list(range(start, end + 1)),
                area=(205, 85, 710, 500),
                stream=True,
                guess=False,
                silent=True,
            )
            fund_to_tables[fund_name] = [tables, fund_name_website, pdf_url]

    all_tables = DF(
        columns=[
            "fund_provider",
            "fund_name_website",
            "fund_name_report",
            "holding_name",
            "currency",
            "market_value",
            "net_assets",
            "pdf_url",
        ]
    )

    for fund_name in fund_to_tables:
        tables, fund_name_website, pdf_url = fund_to_tables[fund_name]
        tables = [x[:] for x in tables]
        tables = [[x, currdir] for x in tables]
        tables = list(map(manage_tables, tables))
        table = pd.concat(tables, ignore_index=True)
        table = table.replace("nan", np.nan)
        table = table[~table["holding_name"].isna()]
        table = table[~(table.isna().all(axis=1))]
        table = table[~table["holding_name"].isin(COUNTRIES[:])]
        table = table[
            ~table["holding_name"].apply(
                lambda x: any(
                    [
                        i in x.lower()
                        for i in [
                            "transferable securities",
                            "total",
                            "exchange",
                            "united states",
                        ]
                    ]
                )
            )
        ]
        table.insert(
            loc=0,
            column="fund_name_website",
            value=fund_name_website,
        )
        table.insert(loc=0, column="fund_name_report", value=fund_name)
        table.insert(loc=0, column="fund_provider", value="Capital At Work")
        table["pdf_url"] = pdf_url
        all_tables = pd.concat([all_tables, table], ignore_index=True)

    all_tables.reset_index(drop=True, inplace=True)
    print(len(all_tables))
    result = DF(
        columns=[
            "fund_provider",
            "fund_name_report",
            "fund_name_website",
            "holding_name",
            "currency",
            "market_value",
            "net_assets",
            "pdf_url",
        ]
    )
    name = ""
    for row in all_tables.iterrows():
        row = row[1]
        name = name + " " + row["holding_name"]
        if not row.isna().any():
            row["holding_name"] = name
            result = result.append(row, ignore_index=True)
            name = ""
    result["market_value"] = result["market_value"].apply(
        lambda x: re.sub(r"[,)( ]", "", str(x))
    )
    result["net_assets"] = result["net_assets"].apply(
        lambda x: re.sub(r"[,)( ]", "", str(x))
    )
    result = result[
        (result["market_value"].apply(lambda x: x.isdigit()))
        & (result["net_assets"].apply(lambda x: x.replace(".", "").isdigit()))
    ]
    result["market_value"] = result["market_value"].apply(
        lambda x: float(x.replace(",", ""))
    )
    result["net_assets"] = result["net_assets"].apply(
        lambda x: float(x.replace(",", ""))
    )
    result = result[
        ~(
            result["holding_name"].apply(
                lambda x: bool(re.search(r"^\s+$|.{90}", x))
            )
        )
    ]
    for idx, row in result.iterrows():
        result["holding_name"][idx] = re.sub(
            r"[^0-9a-zA-Z\/ %)(]", "", row["holding_name"]
        )
        if row["holding_name"].count("%") > 1:
            count = row["holding_name"].count("%")
            text = result["holding_name"][idx]
            s = re.search(r"% {0,3}[\d/.-]+( \w*)*", text)
            if s:
                result["holding_name"][idx] = text[: s.end()]
            else:
                rev = result["holding_name"][idx][::-1]
                result["holding_name"][idx] = rev.replace("%", "", count - 1)[
                    ::-1
                ]

        result["holding_name"][idx] = result["holding_name"][idx].strip()
    result = result[~result["holding_name"].isna()]
    result.insert(loc=3, column="isin", value=None)
    print("All Funds Parsed.\n")
    print("Total - ", len(result))
    result.to_csv(
        os.path.join(currdir, out_path),
        index=False,
        mode="w",
    )


if __name__ == "__main__":
    main()
