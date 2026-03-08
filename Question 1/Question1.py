#importing required libraries
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime as dt
import warnings
warnings.filterwarnings('ignore')

#importing required tables
subscriptions = pd.read_excel("D:\\OneDrive - Cape Breton University\\Cape Breton University\\Semester 4 Subject Material\\Capstone Project\\Project\\Team 1 Project Documents\\A_ABC_DATA\\ABC_Merged_Data_Final.xlsx",sheet_name='A_subscriptions_report_FULL')
invoice = pd.read_excel("D:\\OneDrive - Cape Breton University\\Cape Breton University\\Semester 4 Subject Material\\Capstone Project\\Project\\Team 1 Project Documents\\A_ABC_DATA\\ABC_Merged_Data_Final.xlsx",sheet_name='A_invoice_report_FULL')
transactions = pd.read_excel("D:\\OneDrive - Cape Breton University\\Cape Breton University\\Semester 4 Subject Material\\Capstone Project\\Project\\Team 1 Project Documents\\A_ABC_DATA\\ABC_Merged_Data_Final.xlsx",sheet_name='A_transactions_report_FULL')

#making copy of each table so we have a origianl data to refer back to
sub = subscriptions.copy()
inv = invoice.copy()
trans = transactions.copy()

#Changing column headers to uppercase and removing spaces for standardization
#Giving suffixes to columns of each dataframe to avoid confusion after merges
inv.columns = inv.columns.str.upper().str.replace(" ", "_")
inv = inv.add_suffix("_INV")
sub.columns = sub.columns.str.upper().str.replace(" ", "_")
sub = sub.add_suffix("_SUB")
trans.columns = trans.columns.str.upper().str.replace(" ", "_")
trans = trans.add_suffix("_TRANS")

print(sub.columns.tolist(), sub.dtypes)
print(inv.columns.tolist(), inv.dtypes)
print(trans.columns.tolist(), trans.dtypes)

#removing any spaces from ID columns of all dataframes
id_cols = {"sub" : ["ID_SUB", "PAYER_ID_SUB", "CLIENT_ID_SUB", "PRIMARY_STAFF_ID_SUB"], 
           "inv" : ["INVOICE_INV", "INVOICE_PRIMARY_STAFF_ID_INV", "CLIENT_ID_INV", "SUBSCRIPTION_ID_INV", "SUBSCRIPTION_PRIMARY_STAFF_ID_INV"], 
           "trans" : ["TRANSACTION_ID_TRANS", "PAYER_ID_TRANS", "INVOICE_ID_TRANS", "INVOICE_LINE_ITEM_ID_TRANS"]
           }

for key, cols in id_cols.items():
    temp_df = locals()[key]
    for col in cols:
        if col in temp_df.columns:
            temp_df[col] = temp_df[col].astype(str).str.strip().str.replace(" ", "", regex=False)

#merging tables to create final dataframe
df1 = sub.merge(inv, how="left", left_on=["ID_SUB", "CLIENT_ID_SUB"], right_on=["SUBSCRIPTION_ID_INV", "CLIENT_ID_INV"])
print("After Subscriptions+Invoice merge:",df1.shape)
final_df = df1.merge(trans, how="left", left_on=["INVOICE_INV", "PAYER_ID_SUB"], right_on=["INVOICE_ID_TRANS", "PAYER_ID_TRANS"])
print("After Transactions merge:", final_df.head(), final_df.shape, final_df.columns.tolist())

final_df.to_excel("D:\\OneDrive - Cape Breton University\\Cape Breton University\\Semester 4 Subject Material\\Capstone Project\\Project\\Team 1 Project Documents\\A_ABC_DATA\\ABC_Final_Df_Q1.xlsx", index=False, engine="openpyxl")

print(final_df["SERVICE_SUB"].unique())
print(final_df["CLIENT_ID_SUB"].count())

aggregated_data = final_df.groupby("SERVICE_SUB").agg({
        "CLIENT_ID_SUB": "count",
        "INVOICE_LINE_ITEM_AMOUNT_TRANS": "sum"
    })
print("\nMultiple aggregations by Services:")
print(aggregated_data.sort_values(by="CLIENT_ID_SUB",ascending=False).head())
print(aggregated_data.sort_values(by="CLIENT_ID_SUB",ascending=True).head())
print(aggregated_data.sort_values(by="INVOICE_LINE_ITEM_AMOUNT_TRANS",ascending=False).head())
print(aggregated_data.sort_values(by="INVOICE_LINE_ITEM_AMOUNT_TRANS",ascending=True).head())
 
#Reset index so 'Service' becomes a column for easier plotting
aggregated_data = aggregated_data.reset_index()
 
#Set plot style
sns.set_theme(style="whitegrid", palette="muted")
plt.figure(figsize=(14, 8))

#Bar chart — Top services by number of clients
plt.figure(figsize=(12,6))
top_clients = aggregated_data.sort_values("CLIENT_ID_SUB", ascending=False).head()
ax = sns.barplot(x="CLIENT_ID_SUB", y="SERVICE_SUB", data=top_clients, palette='viridis')
plt.title("Top 5 Services by Number of Clients")
plt.xlabel("Number of Clients")
plt.ylabel("Service")
for container in ax.containers:
    ax.bar_label(container, fmt="%.0f", padding=3)
plt.tight_layout()
plt.show()
 
#Bar chart — Top services by total invoice amount
plt.figure(figsize=(12,6))
top_invoice = aggregated_data.sort_values("INVOICE_LINE_ITEM_AMOUNT_TRANS", ascending=False).head()
ax = sns.barplot(x="INVOICE_LINE_ITEM_AMOUNT_TRANS", y="SERVICE_SUB",data=top_invoice, palette='magma')
plt.title("Top 5 Services by Total Invoice Amount")
plt.xlabel("Total Invoice Amount")
plt.ylabel("Service")
for container in ax.containers:
    ax.bar_label(container, fmt="%.0f", padding=3)
plt.tight_layout()
plt.show()
 
#bar plot — Relationship between clients and total invoice amount
plt.figure(figsize=(10,6))
df_top = aggregated_data.sort_values("INVOICE_LINE_ITEM_AMOUNT_TRANS", ascending=False).head()
ax = sns.barplot(data=df_top, x="SERVICE_SUB", y="INVOICE_LINE_ITEM_AMOUNT_TRANS", palette='Paired')
plt.title("Clients vs Invoice Amount by Service")
plt.xlabel("Service")
plt.ylabel("Total Invoice Amount")
plt.xticks(rotation=45, ha='right')
for container in ax.containers:
    ax.bar_label(container, fmt="%.0f", padding=3)
plt.tight_layout()
plt.show()
 
#Pie chart — Distribution of total invoice amounts across top 8 services
plt.figure(figsize=(8,8))
top_invoice_pie = aggregated_data.sort_values("INVOICE_LINE_ITEM_AMOUNT_TRANS", ascending=False).head()
plt.pie(top_invoice_pie["INVOICE_LINE_ITEM_AMOUNT_TRANS"], labels=top_invoice_pie["SERVICE_SUB"], autopct='%1.1f%%', startangle=140, colors=sns.color_palette('Set3', n_colors=5), pctdistance=0.8)
plt.title("Share of Total Invoice Amount (Top 5 Services)")
plt.tight_layout()
plt.show()

#tail
# Bar chart — Bottom services by number of clients
plt.figure(figsize=(12,6))
bottom_clients = aggregated_data.sort_values("CLIENT_ID_SUB").head()
ax = sns.barplot(x="CLIENT_ID_SUB", y="SERVICE_SUB", data=bottom_clients, palette='viridis')
plt.title("Bottom 5 Services by Number of Clients")
plt.xlabel("Number of Clients")
plt.ylabel("Service")
for container in ax.containers:
    ax.bar_label(container, fmt="%.0f", padding=3)
plt.tight_layout()
plt.show()
 
# Bar chart — Bottom services by total invoice amount
plt.figure(figsize=(12,6))
bottom_invoice = aggregated_data.sort_values("INVOICE_LINE_ITEM_AMOUNT_TRANS").head()
ax = sns.barplot(x="INVOICE_LINE_ITEM_AMOUNT_TRANS", y="SERVICE_SUB", data=bottom_invoice, palette='magma')
plt.title("Bottom 5 Services by Total Invoice Amount")
plt.xlabel("Total Invoice Amount")
plt.ylabel("Service")
for container in ax.containers:
    ax.bar_label(container, fmt="%.0f", padding=3)
plt.tight_layout()
plt.show()