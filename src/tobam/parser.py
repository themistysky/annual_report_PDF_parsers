"""Takes CSV as input. Downloads the PDF files and.

Parses PDFs of provider Tobam. Generates a CSV file
containing parsed results.
"""
import re
import sys
import os
import warnings
import tabula
import pandas as pd
import numpy as np
import requests

warnings.filterwarnings("ignore")

with open("./src/tobam/currencies.txt") as f:
    currencies = f.readline().split()


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
            links_to_pdf_map[link] = "tobam" + str(num) + ".pdf"
            with open("tobam" + str(num) + ".pdf", "wb") as f:
                print("tobam" + str(num) + ".pdf")
                for chunk in response.iter_content(chunk_size=1024):
                    if chunk:
                        f.write(chunk)
            num += 1
        del num
        links_to_pdf_map[np.nan] = np.nan
        links_to_pdf_map["annual_report_does_not_exists"] = np.nan
        pdf_links_df["files"] = pdf_links_df["pdf_url"].apply(
            lambda x: links_to_pdf_map[x]
        )
        pdf_links_df.to_csv("pdf_names.csv", mode="w", index=False)

    fund_to_tables = {}
    file_contents = {}
    for index, row in pdf_links_df.iterrows():
        fund_name_website = row["name"]
        pdf_url = row["pdf_url"]
        isin = row["isin"]
        text = ""
        if "HY" in fund_name_website:
            text = "High Yield"
        if "IG" in fund_name_website:
            text = "Investment Grade"
        read_file = row["files"]
        start, end = 0, 0
        if read_file in file_contents:
            contents = file_contents[read_file]
        else:
            contents = tabula.read_pdf(
                read_file,
                pages=(2),
                area=(280, 50, 835, 590),
                stream=True,
                silent=True,
            )
            contents = contents[0].dropna().reset_index(drop=True)
            contents = list(contents[contents.columns[0]])
            for i in range(len(contents)):
                if not contents[i].split()[-1].isdigit():
                    contents[i] = (
                        contents[i] + " " + contents[i + 1].split()[-1]
                    )
                    contents[i + 1] = "0"
            contents = list(filter(lambda x: x != "0", contents))
        for i in range(len(contents)):
            if re.sub(r"HY|IG", text, fund_name_website) in contents[i]:
                start = int(contents[i].split()[-1])
                end = int(contents[i + 1].split()[-1]) - 1
                fund_name_report = " ".join(contents[i].split()[:-1]).strip()
                break
        if start != 0 and end != 0:
            tables = tabula.read_pdf(
                read_file,
                pages=list(range(start, end + 1)),
                area=(75, 25, 790, 590),
                columns=[355, 454, 541],
                stream=True,
                guess=False,
                silent=True,
            )
            fund_to_tables[fund_name_report] = [
                tables,
                fund_name_website,
                pdf_url,
                isin,
            ]
        print(fund_name_report, ": ", start, end)

    result = pd.DataFrame(
        columns=[
            "fund_name_report",
            "fund_name_website",
            "holding_name",
            "currency",
            "market_value",
            "net_assets",
            "pdf_url",
            "isin",
        ]
    )
    for report in fund_to_tables:
        tables, website, pdf_url, isin = fund_to_tables[report]
        tables = tables[3:-1]
        for i in range(len(tables)):
            tables[i].columns = [
                "holding_name",
                "currency",
                "market_value",
                "net_assets",
            ]
        table = pd.concat(tables, ignore_index=True)
        table = table[table["currency"].isin(currencies)]
        table["holding_name"] = table["holding_name"].apply(
            lambda x: " ".join(x.split()[1:])
        )
        table["market_value"] = table["market_value"].apply(
            lambda x: float(x.replace(",", ""))
        )
        table["net_assets"] = table["net_assets"].apply(lambda x: float(x))
        table["fund_name_report"] = report
        table["fund_name_website"] = website
        table["isin"] = isin
        table["pdf_url"] = pdf_url
        result = pd.concat([result, table], ignore_index=True)
    result["holding_name"] = result["holding_name"].apply(lambda x: x.strip())
    result.insert(
        loc=0, column="fund_provider", value="FOURPOINTS Investment Managers"
    )

    print("Total - ", len(result))
    result.to_csv(
        os.path.join(currdir, os.path.normpath(out_path)),
        index=False,
        encoding="utf-8",
        mode="w",
    )


if __name__ == "__main__":
    main()
