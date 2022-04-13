"""Parse PDF files of Amundi."""
import os.path
import tabula as tb
import pandas as pd
import re
import sys
import PyPDF2
import requests
import warnings

warnings.filterwarnings("ignore")

with open(r"./src/amundi/currencies.txt") as f:
    currencies = f.readline().split()


def find_start_page(pdf, file_name, fund_name):
    """Find page numbers of relevant tables.

    Keyword Argumments:
    pdf -- PdfFileReader() object
    file_name -- PDF file name
    fund_name -- fund name website
    """
    pages = []
    # get number of pages
    num_pages = pdf.getNumPages()
    fund_name = "".join(fund_name.split("-")[-1].lower().split())
    # define keyterms
    my_string = "3.12.InventairedétaillédesinstrumentsfinanciersenEUR"
    # extract text and do the search
    func = (
        lambda i, pdf: pdf.getPage(i)
        .extractText()
        .replace("\n", "")
        .replace(" ", "")
        .lower()
    )
    pages = [
        i + 1
        for i in range(num_pages)
        if my_string.lower() in func(i, pdf) and fund_name in func(i, pdf)
    ]
    # print(pages)
    return pages


def download_file(download_url, filename):
    """Download the file from url.

    Keyword Arguments:
    download_url -- pdf link
    filename -- PDF file name
    """
    response = requests.get(download_url)
    file = open(filename, "wb")
    file.write(response.content)
    file.close()


def process_filedata(file_data, meta_data, i):
    """Process dataframe of a fund."""
    file_data = file_data.drop(2, axis=1)
    file_data = file_data.rename(
        columns={
            0: "holding_name",
            1: "currency",
            3: "market_value",
            4: "net_assets",
        }
    )

    to_keep = [
        "Créances",
        "Dettes",
        "Comptes financiers",
        "Dettes représentatives des titres reçus en garantie",
        "Dettes représentatives des titres données en pension",
    ]
    file_data["currency"] = file_data["currency"].str.strip()
    cond1 = file_data["currency"].isin(currencies)
    cond2 = file_data["holding_name"].isin(to_keep)
    file_data = file_data[cond1 | cond2].reset_index(drop=True)

    def func(x):
        return float(str(x).replace(",", ".").replace(" ", "").strip())

    file_data["market_value"] = file_data["market_value"].apply(func)
    func = (
        lambda x: 0.0
        if str(x) == "nan"
        else float(str(x).replace(",", ".").replace(" ", "").strip())
    )
    file_data["net_assets"] = file_data["net_assets"].apply(func)
    file_data["holding_name"] = file_data["holding_name"].str.strip()

    file_data["fund_name_website"] = file_data[
        "fund_name_report"
    ] = meta_data.iloc[i, 0]
    file_data["pdf_url"] = meta_data["pdf_url"][i]
    return file_data


def find_missing_curr(file_data, text):
    """Find missing currency values of a table."""
    missing_curr = re.search(r"(?<=desinstrumentsfinanciersen).{3}", text)
    if not missing_curr:
        return file_data.currency.mode()[0]
    else:
        return missing_curr.group().upper()


def fill_missing_curr(file_data, missing_curr):
    """Fill rows of missing currency values."""
    for j, r in file_data[1:].iterrows():
        # currency value in rows with holding_name in to_keep
        # have "nan" values. They are filled with the most occuring
        # currency value in table.
        if str(r["currency"]) == "nan":
            file_data["currency"][j] = missing_curr
        if str(r["market_value"]) == "nan":
            file_data["market_value"][j] = 0.0

        # Some holding names are split in two rows, the first
        # one containing the holding name and second one containing
        # date or unimportant characters. In these cases tabula
        # parses the first row with nan values in all other
        # columns(except holding_name) and second row has the
        # values of other columns (with unimportant values in holding_name).
        # Hence only the Second row is kept with its holding_name
        # replaced with the first row.
        if str(r["holding_name"]) == "nan":
            file_data["holding_name"][j] = file_data["holding_name"][j - 1]
    return file_data


def main():
    """Main."""
    assert "-i" in sys.argv, "No input file path provided as argument."
    assert "-o" in sys.argv, "No output file path provided as argument."
    assert "-p" in sys.argv, "No download path provided as argument."
    in_path = sys.argv[sys.argv.index("-i") + 1]
    out_path = sys.argv[sys.argv.index("-o") + 1]
    pdf_out_path = sys.argv[sys.argv.index("-p") + 1]
    currdir = os.getcwd()
    meta_data = pd.read_csv(in_path)
    final_data = []
    os.chdir(os.path.normpath(pdf_out_path))
    for i, row in meta_data.iterrows():
        url = row["pdf_url"]
        file = os.path.join(row["name"] + ".pdf")
        fund_name = row["name"]
        if os.path.isfile(file) == 0:
            print("downloading ", url)
            download_file(url, file)
        else:
            print("downloaded")
        pdf = PyPDF2.PdfFileReader(file)
        pages = find_start_page(pdf, file, fund_name)
        print(file, pages)
        file_data = tb.read_pdf(
            file,
            pages=pages,
            area=(0, 0, 850, 950),
            columns=[320, 360, 420, 512],
            pandas_options={"header": None},
            stream=True,
        )
        file_data = pd.concat(file_data, ignore_index=True)
        file_data = process_filedata(file_data, meta_data, i)
        text = pdf.getPage(pages[0] - 1)
        text = text.extractText()
        text = text.replace("\n", "")
        text = text.replace(" ", "")
        text = text.lower()
        missing_curr = find_missing_curr(file_data, text)
        file_data = fill_missing_curr(file_data, missing_curr)
        if "isin" in meta_data.columns:
            file_data["isin"] = meta_data["isin"][i]

        def func(x):
            return not re.search(r"^Parts", x)

        cond = file_data["holding_name"].apply(func)
        file_data = file_data[cond]
        final_data.append(file_data)
        print("net_assets_sum -", file_data["net_assets"].sum())
        print("market_value_sum -", file_data["market_value"].sum())
        print(len(final_data[-1]))

    final_data = pd.concat(final_data, ignore_index=True)
    final_data["fund_provider"] = "Amundi Asset Management (FR)"
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
    print(
        final_data[
            ["holding_name", "currency", "market_value", "net_assets"]
        ].count()
    )
    final_data = final_data[cols]
    final_data["holding_name"] = final_data["holding_name"].apply(
        lambda x: re.sub(r"[*]", " ", x)
    )
    print("Total rows: ", len(final_data))
    final_data.to_csv(
        os.path.join(currdir, os.path.normpath(out_path)),
        index=False,
        encoding="utf-8",
        mode="w",
    )


if __name__ == "__main__":
    main()
