"""Parse all types of PDFs of financial reports of Four."""
import re
import sys
import os
import warnings
from PyPDF2 import PdfFileReader
import pandas as pd
import numpy as np
import requests
import camelot


warnings.filterwarnings("ignore")


def manage_row(row):
    """Accept a row and call suitable parsing function."""
    if not row:
        return None
    data = row[-1]
    row = row[:-1]
    if len(row) == 10:
        return parse_ten_columns(row, data)
    if len(row) == 11:
        return parse_eleven_columns(row, data)
    if len(row) == 12:
        return parse_twelve_columns(row, data)
    if len(row) == 13:
        return parse_thirteen_columns(row, data)
    if len(row) == 14:
        return parse_fourteen_columns(row, data)
    return None


def parse_fourteen_columns(row, data):
    """Accept a row and return useful data from it."""
    report, website, url = data
    x = row[:]
    return [
        report,
        website,
        x[1],
        x[5],
        x[-4].replace(",", ""),
        x[-1].replace(",", ""),
        url,
    ]


def parse_thirteen_columns(row, data):
    """Accept a row and return useful data from it."""
    report, website, url = data
    x = row[:]
    return [
        report,
        website,
        " ".join(x[0].split()[1:]),
        x[3],
        x[-4].replace(",", ""),
        x[-1].replace(",", ""),
        url,
    ]


def parse_twelve_columns(row, data):
    """Accept a row and return useful data from it."""
    report, website, url = data
    x = row[:]
    if re.search(r"\d", row[0]) and re.search(r"\d", row[1]):
        return [
            report,
            website,
            " ".join(x[0].split()[1:]),
            x[2],
            x[-4].replace(",", ""),
            x[-1].replace(",", ""),
            url,
        ]
    if re.search(r"\d", x[1]):
        return [
            report,
            website,
            x[0],
            x[3],
            x[-4].replace(",", ""),
            x[-1].replace(",", ""),
            url,
        ]
    return [
        report,
        website,
        x[1],
        x[3],
        x[-4].replace(",", ""),
        x[-1].replace(",", ""),
        url,
    ]


def parse_eleven_columns(row, data):
    """Accept a row and return useful data from it."""
    report, website, url = data
    x = row[:]
    if "VERSE" in row:
        return [
            report,
            website,
            x[0],
            x[3],
            x[-3].replace(",", ""),
            x[-1].split()[-1].replace(",", ""),
            url,
        ]
    else:
        return [
            report,
            website,
            " ".join(x[0].split()[1:]),
            x[2],
            x[-4].replace(",", ""),
            x[-1].replace(",", ""),
            url,
        ]


def parse_ten_columns(row, data):
    """Accept a row and return useful data from it."""
    report, website, url = data
    x = row[:]
    if "Cours" not in row:
        return [
            report,
            website,
            x[0],
            x[2],
            x[-4].replace(",", ""),
            x[-1].replace(",", ""),
            url,
        ]
    else:
        return None


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
            links_to_pdf_map[link] = "four" + str(num) + ".pdf"
            with open("four" + str(num) + ".pdf", "wb") as f:
                print("four" + str(num) + ".pdf")
                f.write(response.content)
            num += 1
        del num
        links_to_pdf_map[np.nan] = np.nan
        links_to_pdf_map["annual_report_does_not_exists"] = np.nan
        pdf_links_df["files"] = pdf_links_df["pdf_url"].apply(
            lambda x: links_to_pdf_map[x]
        )
        pdf_links_df.to_csv("pdf_names.csv", mode="w", index=False)

    fund_to_tables_cam = {}
    for index, row in pdf_links_df.iterrows():
        read_file = row["files"]
        fund_name = row["name"]
        pdf = PdfFileReader(read_file)
        total_pages = pdf.getNumPages()
        text1 = "Inventaire sur historique"
        pages = [
            i + 1
            for i in range(1, total_pages)
            if text1 in pdf.getPage(i).extractText().replace("\n", "")
        ]
        tables = camelot.read_pdf(
            read_file,
            pages=",".join(map(str, pages)),
            flavor="stream",
            table_area=",".join(map(str, (110, 1, 577, 835))),
        )
        fund_to_tables_cam[fund_name] = list(tables) + [row["pdf_url"]]
        print("parsing", read_file, fund_name, pages)
    result = []
    for fund_name in fund_to_tables_cam:
        all_tables = []
        tables = fund_to_tables_cam[fund_name]
        pdf_url = tables[-1]
        tables = tables[:-1]
        for table in tables:
            table = table.df[0]
            for row in table:
                if not any([x.isdigit() for x in row]):
                    continue
                text = list(filter(lambda x: x != "", row.split("  ")))
                text = [x.strip() for x in text]
                text.append([fund_name, fund_name, pdf_url])
                all_tables.append(text)
        result.extend(all_tables)
    del fund_to_tables_cam, all_tables
    result = list(map(manage_row, result))
    result = list(filter(lambda x: x is not None, result))
    result = pd.DataFrame(
        result,
        columns=[
            "fund_name_report",
            "fund_name_website",
            "holding_name",
            "currency",
            "market_value",
            "net_assets",
            "pdf_url",
        ],
    )
    result.insert(0, "fund_provider", value="four")
    result.insert(3, "isin", value=None)
    result["holding_name"] = result["holding_name"].apply(lambda x: x.strip())
    result["currency"] = result["currency"].apply(lambda x: x.strip())
    result = result[
        ~result["holding_name"].apply(lambda x: str(x) in ["nan", ""])
    ]
    result["market_value"] = result["market_value"].apply(
        lambda x: float(x.strip())
    )
    result["net_assets"] = result["net_assets"].apply(
        lambda x: float(x.strip())
    )
    result = result[
        result["currency"].apply(lambda x: bool(re.search(r"[ A-Z]", str(x))))
    ]
    result.reset_index(drop=True, inplace=True)

    print("Total - ", len(result))
    result.to_csv(
        os.path.join(currdir, os.path.normpath(out_path)),
        index=False,
        encoding="utf-8",
        mode="w",
    )


if __name__ == "__main__":
    main()
