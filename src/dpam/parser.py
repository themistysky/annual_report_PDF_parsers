"""Takes CSV as input. Downloads the PDF files and.

Parses PDFs of provider DPAM. Generates a CSV file
containing parsed results.
"""
import re
import sys
import os
import warnings
import tabula
import PyPDF2
import pandas as pd
import numpy as np
import requests

warnings.filterwarnings("ignore")

with open("./src/dpam/currencies.txt") as f:
    currencies = f.readline().split()


def findPages1(file, name):
    """Get page numbers with the relevant tables.

    Keyword Arguments:
    file-- file name
    name-- fund name website
    """
    pdf = PyPDF2.PdfFileReader(file)
    pages = []
    NumPages = pdf.getNumPages()

    name = (
        name.split("-")[0]
        .split("L ")[-1]
        .split("B ")[-1]
        .lower()
        .replace(" ", "")
    )

    s1 = (
        name
        + "investmentquantityccycost(ineur)evaluationvalue(ineur)%totalnetassets"
    )

    check = False
    for i in range(0, NumPages):
        PageObj = pdf.getPage(i)
        Text = PageObj.extractText()
        Text = Text.replace("\n", " ").replace(" ", "").lower()
        if s1 in Text:
            pages.append(i + 1)
            check = True
        elif check == True:
            break

    return pages


def findPages2(file, name):
    """Get page numbers with the relevant tables.

    Keyword Arguments:
    file-- file name
    name-- fund name website
    """
    pdf = PyPDF2.PdfFileReader(file)
    pages = []

    NumPages = pdf.getNumPages()

    name = name.split("-")[0].lower().replace(" ", "")

    s1 = [
        name,
        "descriptionquantityat31december2020currencypriceincurrencyevaluationeur%bytheuci%portfolio%netassets",
    ]
    check = False
    for i in range(0, NumPages):
        PageObj = pdf.getPage(i)
        Text = PageObj.extractText()
        Text = Text.replace("\n", " ").replace(" ", "").lower()
        if all(x in Text for x in s1):
            pages.append(i + 1)
            check = True
        elif check == True:
            break
    return pages


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
    meta_data = pd.read_csv(os.path.join(currdir, os.path.normpath(in_path)))
    links_set = set(meta_data["pdf_url"])
    links_set.discard("annual_report_does_not_exists")
    os.chdir(os.path.join(currdir, os.path.normpath(pdf_out_path)))
    if "file_names" not in meta_data.columns:
        links_set = set(meta_data["pdf_url"])
        links_set.discard("nan")
        links_set.discard("annual_report_does_not_exists")
        links_to_pdf_map = {}
        num = 1
        print("Downloading " + str(len(links_set)) + " files...")
        for link in links_set:
            if link in ["nan", "annual_report_does_not_exists"]:
                continue
            response = requests.get(link)
            links_to_pdf_map[link] = "dpam" + str(num) + ".pdf"
            with open("dpam" + str(num) + ".pdf", "wb") as f:
                print("dpam" + str(num) + ".pdf")
                for chunk in response.iter_content(chunk_size=1024):
                    if chunk:
                        f.write(chunk)
            num += 1
        del num, response
        links_to_pdf_map[np.nan] = np.nan
        links_to_pdf_map["annual_report_does_not_exists"] = np.nan
        meta_data["file_names"] = meta_data["pdf_url"].apply(
            lambda x: links_to_pdf_map[x]
        )
        meta_data.to_csv("pdf_names.csv", mode="w", index=False)

    fnr_dict = {}
    final_data = []
    for i in range(len(meta_data)):
        file = meta_data["file_names"][i]
        df = (
            tb.read_pdf(
                file,
                pages=1,
                area=(100, 30, 740, 630),
                pandas_options={"header": None},
                stream=True,
                silent=True,
            )[0]
            .dropna()
            .reset_index(drop=True)
        )
        if str(df[0][0]).startswith("DPAM"):
            s = meta_data["name"][i]
            print("parsing", file, s, end="  ")
            file = "dpam" + str(i + 1) + ".pdf"
            fnr = s.split("-")[0].split("L ")[-1].split("B ")[-1]
            if fnr not in fnr_dict:
                pages = findPages1(file, s)
                print(pages)
                if pages == []:
                    print("no pages found")
                    continue

                pages_data = []
                for p in pages:
                    df = tb.read_pdf(
                        file,
                        pages=p,
                        area=(150, 50, 770, 630),
                        columns=[280, 341, 354, 450, 500],
                        pandas_options={"header": None},
                        stream=True,
                        silent=True,
                    )[0]

                    if ~df.empty:
                        pages_data.append(df)

                fund_name = pd.concat(pages_data)
                fund_name = fund_name.drop([1, 3], axis=1)
                fund_name = fund_name.rename(
                    columns={
                        2: "currency",
                        5: "net_assets",
                        0: "holding_name",
                        4: "market_value",
                    }
                )
                fund_name = fund_name[
                    fund_name["currency"]
                    .replace(np.nan, "NA")
                    .isin(currencies)
                ]
                fund_name["fund_provider"] = "DPAM"
                fund_name["fund_name_report"] = fnr
                fund_name["isin"] = None
                fund_name["pdf_url"] = meta_data["pdf_url"][i]
                fund_name["market_value"] = fund_name["market_value"].apply(
                    lambda x: float(str(x).replace(",", ""))
                )
                fund_name["net_assets"] = fund_name["net_assets"].apply(
                    lambda x: float(str(x).replace("%", ""))
                )
                fnr_dict[fnr] = fund_name.copy()

            else:
                print("memory")
                fund_name = fnr_dict[fnr]

            fund_name["fund_name_website"] = s
            cols = [
                "fund_provider",
                "fund_name_report",
                "fund_name_website",
                "isin",
                "holding_name",
                "currency",
                "market_value",
                "net_assets",
                "pdf_url",
            ]
            fund_name = fund_name[cols]
            final_data.append(fund_name)
            print("net_assets_sum:", fund_name["net_assets"].sum())

        else:
            fnw = meta_data["name"][i]
            print("parsing", file, fnw, end="  ")
            file = "dpam" + str(i + 1) + ".pdf"
            fnr = fnw.split("-")[0].split("L ")[-1].split("B ")[-1]
            if fnr not in fnr_dict:
                pages = findPages2(file, fnw)
                print(pages)
                if pages == []:
                    print("no pages found")
                    continue

                pages_data = []
                for p in pages:
                    df = tb.read_pdf(
                        file,
                        pages=p,
                        area=(100, 50, 750, 630),
                        columns=[220, 294, 306, 360, 430, 500],
                        pandas_options={"header": None},
                        stream=True,
                        silent=True,
                    )[0]

                    if ~df.empty:
                        pages_data.append(df)

                fund_name = pd.concat(pages_data)
                fund_name = fund_name.drop([1, 3], axis=1)
                fund_name = fund_name.rename(
                    columns={
                        2: "currency",
                        6: "net_assets",
                        0: "holding_name",
                        4: "market_value",
                    }
                )
                fund_name = fund_name[
                    fund_name["currency"]
                    .replace(np.nan, "NA")
                    .isin(currencies)
                ]
                fund_name["fund_provider"] = "DPAM"
                fund_name["fund_name_report"] = fnr
                fund_name["isin"] = None
                fund_name["pdf_url"] = meta_data["pdf_url"][i]
                fund_name["market_value"] = fund_name["market_value"].apply(
                    lambda x: float(str(x).replace(",", ""))
                )
                fund_name["net_assets"] = fund_name["net_assets"].apply(
                    lambda x: float(str(x).replace("%", ""))
                )
                fnr_dict[fnr] = fund_name.copy()

            else:
                print("memory")
                fund_name = fnr_dict[fnr]

            fund_name["fund_name_website"] = fnw
            cols = [
                "fund_provider",
                "fund_name_report",
                "fund_name_website",
                "isin",
                "holding_name",
                "currency",
                "market_value",
                "net_assets",
                "pdf_url",
            ]
            fund_name = fund_name[cols]
            final_data.append(fund_name)
            print("net_assets_sum:", fund_name["net_assets"].sum())

    final_data = pd.concat(final_data).reset_index(drop=True)
    cols = [
        "fund_provider",
        "fund_name_report",
        "fund_name_website",
        "isin",
        "holding_name",
        "currency",
        "market_value",
        "net_assets",
        "pdf_url",
    ]
    final_data = final_data[cols]
    print("Total - ", len(result))
    final_data.to_csv(
        os.path.join(currdir, os.path.normpath(out_path)),
        index=False,
        encoding="utf-8",
        mode="w",
    )


if __name__ == "__main__":
    main()
