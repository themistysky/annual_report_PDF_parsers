# annual_report_PDF_parsers

## Function
Parse the annual reports in PDF format of investment/ asset management companies and export to CSV files.

## Input
The input is a CSV file in input_files/<fund_provider_code>.csv with columns:

- `name`: the fund name
- `pdf_url`: the URL from where to fetch the fund's annual report
- `isin` (optional): a unique number identifying the fund, useful if `name`'s are not uniques


### Example

| **name**                                                         | **pdf_url**                                                                                                               | **isin**     |
| ---------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------- | ------------ |
| Aviva Investors Global Convertibles Fund Iah GBP Inc             | https://doc.morningstar.com/document/3bbb01b8cb5c4ecf8ca0f3f12d5aa846.msdoc/?clientId=avivainvestors&key=0011b526a18a80ef | LU0367993150 |
| Aviva Investors European Corporate Bond Fund I EUR Acc           | https://doc.morningstar.com/document/3bbb01b8cb5c4ecf8ca0f3f12d5aa846.msdoc/?clientId=avivainvestors&key=0011b526a18a80ef | LU0160771357 |
| Aviva Investors Global Aviva France Global High Yield Fund K EUR | https://doc.morningstar.com/document/8b359978a2c0c810eafc0e2f90ddd3d9.msdoc/?clientId=avivainvestors&key=0011b526a18a80ef | LU2202899741 |


## Output

The output is a CSV table with columns:

- `fund_provider` (`str`): fund provider code
- `fund_name_report` (`str`): the fund name as stated in the PDF report. Must be the same as, or very similar to the `name` in the input file
- `fund_name_website` (`str`): the fund name as stated in CSV input file. Must be exactly the same as the `name` in the input file
- `isin` (`str`): a unique number identifying the fund, useful if `fund_name_report` or `fund_name_website` are not uniques. If column `isin` is not in the input file, then fill the column with `None`values
- `holding_name` (`str`): name of the financial commodity which is part of the portfolio of the fund
- `market_value` (`float`): amount invested in the financial commodity
- `currency` (`str`): the currency of the amount invested specified in `market_value`
- `net_assets` (`float`): the share of the value that the financial commodity represents within the fund, in percent
- `pdf_url`: the URL from where to fetch the fund's annual report, copied from the input file


Make sure that the columns have the rights types.

### Example

| **fund_provider** | **fund_name_report**                       | **fund_name_website**                                | **isin**     | **holding_name**                   | **market_value** | **currency** | **net_assets** | **pdf_url**                                                                                                               |
| ----------------- | ------------------------------------------ | ---------------------------------------------------- | ------------ | ---------------------------------- | ---------------- | ------------ | -------------- | ------------------------------------------------------------------------------------------------------------------------- |
| Aviva             | Aviva Investors - Global Convertibles Fund | Aviva Investors Global Convertibles Fund Iah GBP Inc | LU0367993150 | China Education GroupHoldings Ltd. | 30 000 000.00    | HKD          | 1.18           | https://doc.morningstar.com/document/3bbb01b8cb5c4ecf8ca0f3f12d5aa846.msdoc/?clientId=avivainvestors&key=0011b526a18a80ef |
| Aviva             | Aviva Investors - Global Convertibles Fund | Aviva Investors Global Convertibles Fund Iah GBP Inc | LU0367993150 | Harvest International Co.          | 20 000 000.00    | HKD          | 0.98           | https://doc.morningstar.com/document/3bbb01b8cb5c4ecf8ca0f3f12d5aa846.msdoc/?clientId=avivainvestors&key=0011b526a18a80ef |
| Aviva             | Aviva Investors - Global Convertibles Fund | Aviva Investors Global Convertibles Fund Iah GBP Inc | LU0367993150 | Kingsoft Corp. Ltd.                | 15 000 000.00    | HKD          | 0.76           | https://doc.morningstar.com/document/8b359978a2c0c810eafc0e2f90ddd3d9.msdoc/?clientId=avivainvestors&key=0011b526a18a80ef |



## Program Flow

1. Open the input CSV file
2. Download the PDF linked to the fund
3. In the PDF, find the exact table about the fund, as a single report may contain info about multiple funds
4. Parse the found table
5. Format the table according to the output format
6. Save the table as CSV




  - works from a fresh project, to do so
    - clone the repo , e.g.
      - `git clone git@github.com:Wequity/FUSA_pdf_parser_py.git fresh_test`
      - `cd fresh_test`
    - create a new environment and install all dependencies, e.g.
      - create the environment `python3 -m venv .env`
      - activate it `.env/bin/activate `
      - install the requirements `pip install -r requirements.txt`
    - create outputs folder if needed, e.g.
      - `mkdir pdf`
    - run yout code, e.g.
      - `FUSA_pdf_parser_py % python src/aviva/parser.py -i input_files/aviva.csv -o res/aviva_results.csv -p pdf`
   

### Usage

Each parser should have the following arguments:

- `-i input_file_path` the path to the CSV input file, with format as specified above
  - e.g. `-i input_files/aviva.csv`
- `-o output_file_path` the path to the CSV output file which will be created by the parser, with format as specified above
  - e.g. `-o res/aviva_results.csv`
- `-p pdf_folder_path` the path to the folder in which to store downloaded PDF reports, if needed
  - e.g. `-p pdf`

So, e.g., the script would be executed with the command 
`FUSA_pdf_parser_py % python src/aviva/parser.py -i input_files/aviva.csv -o res/aviva_results.csv -p pdf`
