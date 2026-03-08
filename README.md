# Capestone-Project-Music-Academy-
Analyzing a Music academy's performance, we integrated subscription, invoice, and attendance data to assess service metrics and demo-to-paid conversions. This data-driven assessment identifies key patterns in payment behaviour and client engagement to optimise revenue and reduce default payments. This is private data; it cannot be disclosed without the company's authorisation. And so, the results cannot be shared either.

1) INTRODUCTORY INFORMATION
This project comprises three analytical modules (Q1, Q2, Q3) that focus on service performance, sales behaviour, and conversion analytics to support business decision-making.

1.1. Project Overview
This project analyzes client behavior and revenue for an organization using four main tables stored in a single Excel workbook:

- Attendance report (‘A_attendance_report_FULL’)
- Subscriptions report (‘A_subscriptions_report_FULL’)
- Invoice report (‘A_invoice_report_FULL’)
- Transactions report (‘A_transactions_report_FULL’)

The scripts perform the following:

Question1.py’ – Service & Revenue Overview
- Loads subscriptions, invoices, and transactions sheets.
- Cleans and standardizes key ID columns (removes spaces, trims, uppercases column headers).
- Merges:
o ‘subscriptions’ ⟶ ‘invoices’ (on subscription & client IDs)  
o Result ⟶ ‘transactions’ (on invoice and payer IDs)
- Aggregates at the service level (‘SERVICE_SUB’):
o Number of clients per service
o Total invoice amounts per service
- Generates visualizations:
o Top and bottom services by client count
o Top and bottom services by total invoice amount
o Pie chart of invoice amount share for top services
- Also explores relationship between client volume and revenue per service.

Question2.py – Late Payments & Monthly Trends
- Loads invoice and transactions sheets.
- Merges transactions with invoices on ‘Invoice Id_trans’ = ‘Invoice_invoice’.
- Robustly parses mixed date formats for:
o ‘Date posted_trans’
o ‘Invoice Due Date_trans’
- Flags late transactions (‘is_late = 1’ if posted date > due date).
- Filters to late transactions and aggregates by:
o ‘Service_trans’
o ‘Location_trans’
o ‘month_name_full’
- Generates visualizations for late transactions:
o Total late transaction amount by service
o Monthly stacked bar of transaction amount by service
o Monthly sales trend line chart (late transactions)
o Service vs location heatmap
o Dualaxis chart: amount vs count by service
- Location-based performance” and “Monthly and service-based sales patterns

Question3.py – Demo Conversion, Revenue & Retention
- Loads all four sheets: attendance, subscriptions, invoices, transactions.
- Standardizes:
o Column names (upper case, underscore, suffix ‘_*_ATT/SUB/INV/TRANS’)
o ID columns (trim, remove spaces, handle empty strings)
o Date columns (normalized to date only)
- Identifies “demo” or “free” services using keywords:
o ‘FREE’, ‘DEMO’, ‘TRIAL’, ‘TRY’ in service names.
- Builds a demo client cohort:
- Union of demo clients from attendance and invoices.
- Determines if they converted (appear in subscriptions as nonfree services).
- Computes:
o First demo date (‘FIRST_DEMO_DATE’)
o First paid subscription date (‘FIRST_PAID_DATE’)
o ‘DAYS_TO_CONVERT’
o ‘CONVERTED_WITHIN_WINDOW’ (conversion within ‘CONVERSION_WINDOW_DAYS = 180’)
- Revenue attribution:
o Cleans numeric transaction amounts.
o Filters to successful positive transactions.
o Maps transactions to demo clients, and then to converted clients.
o Computes:
 Total revenue from converted clients
 Revenue after conversion date
 Revenue per client
 Revenue per service
 Revenue per location
- Additional analytics:
o Conversion by demo source (Attendance, Invoice, Both)
o Conversions by location
o Retention via ‘CLIENT_STATUS_SUB’ (e.g., Member vs Member Lost)
o Conversion clusters by days to convert: ‘03’, ‘47’, ‘8+’, ‘NO_CONVERSION’
o Conversion rate by demo weekday and demo month
- Saves multiple CSV outputs under ‘outputs/’ directory.
- Visual trends including conversion by weekday/month and conversion cohorts over time.

2) SOFTWARE AND PACKAGE VERSIONS
The code is written for Python 3.x (recommended: 3.9+). Windows OS recommended because script paths are in Windows format. The following packages are required:

Python >= 3.9

Required libraries:
- Numpy >= 1.23
- pandas >= 1.5
- matplotlib >= 3.6
- seaborn >= 0.12
- pythondateutil >= 2.8 (for ‘from dateutil import parser’ in Question2.py & Question3.py)
- openpyxl >= 3.1 (for reading/writing Excel files)

2.1. Installation
Using pip:
pip install numpy pandas matplotlib seaborn pythondateutil openpyxl

3) HOW TO RUN THE CODE
Important: All three scripts assume that the Excel file path and sheet names are correct and accessible from your machine.

3.1. Recommended Execution Order
The scripts are logically independent (each can be run on its own), but for a typical analysis workflow:

Service & Revenue Overview:
python Question1.py
• Produces merged dataset and service-level visualisations.

Late Payment Analysis:
python Question2.py
• Produces late payment analytics and plots by service, location, and month.

Conversion & Revenue Funnel:
python Question3.py
• Produces the full funnel demo → conversion → revenue analysis.
• Writes multiple CSV outputs into an outputs directory.

3.2. Output Files
Question1.py

• Writes: 
o ABC_Final_Df_Q1.xlsx to the same A_ABC_DATA folder (path is hardcoded).
• Shows multiple plots (bar charts and pie charts).

Question2.py
• Does not write new files by default, but:
o Prints summary tables to the console.
o Opens several plots in new windows:
o Bar charts
o Stacked bar
o Line chart
o Heatmap
o Dual-axis chart

Question3.py
• Writes multiple CSV files
Files include (but are not limited to):

• cohort_summary_final.csv
• demo_clients_summary.csv
• location_conversion_summary_final.csv
• rev_by_location_final.csv
• rev_by_client_converted_final.csv
• rev_by_service_final.csv
• status_for_converted_clients.csv
• service_conversion_counts_final.csv
• trans_demo_success.csv
• status_summary_final.csv

• Shows many visualisations:
o Overall conversion bars
o Conversion by demo source
o Days to convert histogram
o Conversion cluster distribution
o Top converted clients by revenue
o Top services by conversion & revenue
o Location-based conversion and revenue
o Conversion rate by demo weekday and demo month
o Retention chart for converted clients

If the outputs directory does not exist, you may need to create it manually or adjust OUTPUT_DIR in Question3.py.

4) TESTING
There are no formal automated unit tests, but you can verify that the scripts are working as expected using the following manual / sanitycheck tests:

4.1. Data Loading & Shape Checks
• Ensure each script successfully loads the Excel file:
o No FileNotFoundError.
o No XLRDError or engine errors.

• Check printed shapes after merges in Question1.py:
o After Subscriptions+Invoice merge: 
o After Transactions merge: ...

• In Question3.py, look at:
o Counts of demo clients, converted clients, and rows in first_demo, first_paid.

4.2. Basic Logical Validation
• Question1.py
o Verify that SERVICE_SUB is populated (not just “nan”).
o Confirm that the number of unique services in aggregated_data makes sense.
o Check that the top services by client count and by invoice amount are reasonable given expectations.

• Question2.py
o Confirm that Date posted_trans_parsed and Invoice Due Date_parsed are not all NaT.
o Verify that some records are flagged with is_late = 1 (unless all invoices are ontime).
o Check that the month_name_full column contains valid month names.

• Question3.py
• Confirm that:
o demo_clients_df has a mix of CONVERTED = True/False (if data supports it).
o DAYS_TO_CONVERT values are nonnegative for converted clients.
o CONVERTED_WITHIN_WINDOW counts match expectations for a 180-day window.
• Check summaries:
o Overall conversion rate.
o Source-based conversion rates.
o Revenue totals for converted clients.

4.3. Output Integrity
• Open selected CSV outputs (e.g., cohort_summary_final.csv, rev_by_client_converted_final.csv) and verify:
o No obvious malformed values (e.g., all zeros for amounts when you expect non-zero).
o Headers are correct and match the intent described in the code.
o Date fields are in consistent formats.

• If you want more formal testing, you can:
• Wrap parts of the logic into reusable functions.
• Add small sample test datasets and write pytest tests that validate:
o Known counts (e.g., known number of late invoices).
o Known conversion behavior for test clients.

4.4. Visualization Tests: 
• Confirm all charts render without errors.

5) KNOWN ISSUES
The following are known limitations / potential issues in the current scripts:
1. HardCoded File Paths
• All file paths are hardcoded for a specific Windows environment and OneDrive structure.
• If you move the project or run it on another machine, you must edit:
o Paths in Question1.py & Question2.py.
o INPUT_XLSX and OUTPUT_DIR in Question3.py.

2. Mixed Date Formats
• Question2.py uses a custom parse_mixed_date() function plus dateutil.parser.
• If there are date strings in formats not in the provided formats list, they may fall back to parser.parse, or to NaT.
• Ambiguous dates (e.g., 01/02/2023) might be interpreted as MM/DD/YYYY (dayfirst=False), which may not align with your local convention.

3. Suppressed Warnings
• warnings.filterwarnings('ignore') is used globally in all scripts.
• This can hide useful warnings (like chained assignment warnings in pandas).
• For debugging or production hardening, consider removing this or selectively enabling warnings.

4. Memory & Performance
• The scripts read full sheets into memory and perform multiple merges and groupby operations.
• For very large datasets, you may experience slow performance or high memory usage.
• Potential improvements:
o Load only relevant columns.
o Use chunking or an intermediate database.

5. Extension Mismatch for to_csv
• In Question3.py, this line: demo_union_full.to_csv(os.path.join(OUTPUT_DIR, "demo_union_full.xlsx"), index=False) writes a CSV file with a .xlsx extension (not actual Excel format).
• This can confuse Excel and users. Fix suggestion: change the file name to "demo_union_full.csv" or use to_excel if you truly want an Excel file.

6. Locale / Currency Formatting
• Amounts are treated as raw numeric floats.
• Currency formatting is only applied in labels in plots, not stored in the data itself.
• No explicit handling is done for different locales (e.g., commas vs dots).

7. Assumptions About Business Logic
• “Demo” is identified purely by keywords: FREE, DEMO, TRIAL, TRY in service names.
• “Successful” transaction is identified by TRANSACTION_STATUS_TRANS == 'SUCCESSFUL' and positive amount.
• These assumptions may not match all real-world edge cases.

8. Excel dependency: missing or renamed sheets may break merging.

6) SOURCES, REFERENCES, and ATTRIBUTIONS
This project is based on:

• Client Dataset (internal):
o ABC_Merged_Data_Final.xlsx
o Sheets:
o Attendance report (A_attendance_report_FULL)
o Subscriptions report (A_subscriptions_report_FULL)
o Invoice report (A_invoice_report_FULL)
o Transactions report (A_transactions_report_FULL)

• Python Libraries & Documentation:
o NumPy documentation
o pandas documentation
o Matplotlib documentation
o Seaborn documentation
o pythondateutil documentation
o openpyxl documentation

• General Techniques:
o Data cleaning and normalization patterns (uppercasing columns, trimming whitespace).
o Standard practice for converting mixedformat date strings using pandas.to_datetime and dateutil.parser.
o Common visualization patterns with Matplotlib/Seaborn (bar plots, line plots, histograms, heatmaps, dualaxis charts).
