import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime as dt
import warnings
import calendar as cal
import pandas as pd
from dateutil import parser
warnings.filterwarnings('ignore')

invoice = pd.read_excel("D:\\OneDrive - Cape Breton University\\Cape Breton University\\Semester 4 Subject Material\\Capstone Project\\Project\\Team 1 Project Documents\\A_ABC_DATA\\ABC_Merged_Data_Final.xlsx",sheet_name='A_invoice_report_FULL')
invoice_suff='_invoice'
invoice=invoice.add_suffix(invoice_suff)
trans = pd.read_excel("D:\\OneDrive - Cape Breton University\\Cape Breton University\\Semester 4 Subject Material\\Capstone Project\\Project\\Team 1 Project Documents\\A_ABC_DATA\\ABC_Merged_Data_Final.xlsx",sheet_name='A_transactions_report_FULL')
trans_suff='_trans'
trans=trans.add_suffix(trans_suff)
final_df = trans.merge(invoice,how='left',left_on='Invoice Id_trans',right_on='Invoice_invoice')
#df['Date_Column'].dt.strftime('%d-%m-%Y')

formats = [
    '%d-%m-%Y',
    '%Y-%m-%d',
    '%m/%d/%Y %I:%M:%S %p',
    '%m/%d/%Y',
    '%d/%m/%Y %H:%M:%S',
    '%d/%m/%Y',
    '%Y/%m/%d %H:%M:%S',
    # add any other formats you expect
]

def parse_mixed_date(s):
    if pd.isna(s):
        return pd.NaT
    if isinstance(s, pd.Timestamp):
        return s
    s = str(s).strip()
    # try exact formats first
    for fmt in formats:
        try:
            return pd.to_datetime(s, format=fmt)
        except Exception:
            continue
    # fallback to dateutil (smart/infer)
    try:
        # you can pass dayfirst=True if you prefer that interpretation
        return parser.parse(s, dayfirst=False)  
    except Exception:
        return pd.NaT

final_df['Date posted_trans_parsed'] = final_df['Date posted_trans'].apply(parse_mixed_date)
final_df['Invoice Due Date_parsed'] = final_df['Invoice Due Date_trans'].apply(parse_mixed_date)
final_df['date_diff'] = final_df['Date posted_trans_parsed'] - final_df['Invoice Due Date_parsed']
final_df['is_late'] = np.where(final_df['date_diff'].dt.days > 0, 1, 0)
print(final_df.columns)
print(final_df.dtypes)
print(final_df.head())
print(final_df[final_df['is_late']==1])
Is_late_df = final_df[final_df['is_late']==1]
print(Is_late_df.head())
#print(final_df[final_df['Transaction Id_trans']=='0395b56f'])
print(Is_late_df['Transaction Amount_trans'].sum())
Is_late_df['month_name_full'] = Is_late_df['Date posted_trans'].dt.month_name()
aggregated_data = Is_late_df.groupby(["Service_trans","Location_trans","month_name_full"]).agg({
        "Transaction Amount_trans": "sum",
        "Transaction Id_trans": "count"
    })
print("\nMultiple aggregations by Services:")
print(aggregated_data.sort_values(by="Transaction Amount_trans",ascending=False).head())
print(aggregated_data.sort_values(by="Transaction Amount_trans",ascending=True).head())
print(aggregated_data.sort_values(by="Transaction Id_trans",ascending=False).head())
print(aggregated_data.sort_values(by="Transaction Id_trans",ascending=True).head())

aggregated_data.reset_index(inplace=True)

#Visualization Annotations
LABEL_FONTSIZE = 11
LABEL_FONTWEIGHT = 'bold'
LABEL_COLOR = 'black'

def annotate_bars(ax, fmt="{:.0f}", offset_rel=0.02,
                  fontsize=LABEL_FONTSIZE, fontweight=LABEL_FONTWEIGHT, color=LABEL_COLOR):
    patches = [p for p in ax.patches if np.isfinite(p.get_height())]
    if not patches:
        return
    maxh = max([p.get_height() for p in patches])
    offset = maxh * offset_rel if maxh != 0 else offset_rel

    for p in patches:
        h = p.get_height()
        if not np.isfinite(h):
            continue
        x = p.get_x() + p.get_width()/2
        label = fmt.format(h)
        ax.text(x, h + offset, label, ha='center', va='bottom',
                fontsize=fontsize, fontweight=fontweight, color=color)

def annotate_percent(ax, decimals=1, offset_rel=0.02,
                     fontsize=LABEL_FONTSIZE, fontweight=LABEL_FONTWEIGHT, color=LABEL_COLOR):
    patches = [p for p in ax.patches]
    if not patches:
        return
    maxh = max([p.get_height() for p in patches])
    offset = maxh * offset_rel if maxh != 0 else offset_rel

    for p in patches:
        h = p.get_height()
        x = p.get_x() + p.get_width()/2
        ax.text(x, h + offset, f"{h:.{decimals}f}%", ha='center',
                fontsize=fontsize, fontweight=fontweight, color=color)

def annotate_hist(ax, fmt="{:.0f}", offset_rel=0.02,
                  fontsize=LABEL_FONTSIZE, fontweight=LABEL_FONTWEIGHT, color=LABEL_COLOR):
    patches = ax.patches
    if not patches:
        return
    maxh = max([p.get_height() for p in patches])
    offset = maxh * offset_rel if maxh != 0 else offset_rel

    for p in patches:
        h = p.get_height()
        x = p.get_x() + p.get_width()/2
        ax.text(x, h + offset, fmt.format(h), ha='center', va='bottom',
                fontsize=fontsize, fontweight=fontweight, color=color)

# Bar chart: total sales by Service
plt.figure(figsize=(9,5))
ax = aggregated_data.groupby('Service_trans')['Transaction Amount_trans'].sum().sort_values().plot(kind='bar', title='Total Transaction Amount by Service', color='steelblue')
plt.ylabel('Total Amount')
annotate_bars(ax)
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.show()

#Stacked Bar Chart — Compare services across locations or months
pivot = aggregated_data.pivot_table(index='month_name_full', columns='Service_trans', values='Transaction Amount_trans', aggfunc='sum')

plt.figure(figsize=(12,6))
ax = pivot.plot(kind='bar', stacked=True, figsize=(12,6), title='Monthly Transaction Amount by Service')

plt.ylabel('Amount')
plt.xticks(rotation=45, ha='right')
plt.legend(title='Service', bbox_to_anchor=(1.05, 1), loc='upper left')
plt.tight_layout()
plt.show()

#Line Chart / Time Series Trend — Trend over months
month_order = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
trend = aggregated_data.groupby('month_name_full')['Transaction Amount_trans'].sum()
trend = trend.reindex(month_order).dropna()
x = range(len(trend))
y = trend.values
plt.figure(figsize=(9,5))
plt.plot(x, y, marker='o', linewidth=2, color='darkblue')
plt.title('Monthly Sales Trend')
plt.ylabel('Transaction Amount')
plt.xticks(x, trend.index, rotation=45, ha='right')
for i, val in enumerate(y):
    plt.text(i, val, f"{val:.0f}", ha='center', va='bottom', fontsize=LABEL_FONTSIZE, fontweight=LABEL_FONTWEIGHT)
plt.tight_layout()
plt.show()

#Heatmap — Relationship between Service and Location
heatmap_data = aggregated_data.pivot_table(
    index='Service_trans',
    columns='Location_trans',
    values='Transaction Amount_trans',
    aggfunc='sum')

plt.figure(figsize=(12,7))
sns.heatmap(heatmap_data, annot=True, fmt='.0f', cmap='YlGnBu')
plt.xticks(rotation=45, ha='right')
plt.title('Transaction Amount by Service and Location')
plt.tight_layout()
plt.show()

#Dual-Axis Chart — Amount vs Count
service_summary = aggregated_data.groupby('Service_trans').agg({
    'Transaction Amount_trans': 'sum',
    'Transaction Id_trans': 'sum'
}).reset_index()

fig, ax1 = plt.subplots(figsize=(12,6))

#Bar (Amount)
bars = ax1.bar(service_summary['Service_trans'], service_summary['Transaction Amount_trans'], alpha=0.65, color='skyblue', label='Transaction Amount')
ax2 = ax1.twinx()

#Line (Count)
ax2.plot(service_summary['Service_trans'], service_summary['Transaction Id_trans'], color='crimson', marker='o', linewidth=2, label='Transaction Count')
ax1.set_xlabel('Service')
ax1.set_ylabel('Total Transaction Amount', color='blue')
ax2.set_ylabel('Transaction Count', color='red')
ax1.set_xticklabels(ax1.get_xticklabels(), rotation=45, ha='right')
plt.title('Transaction Amount vs Count by Service')

#annotate bars
annotate_bars(ax1)

#annotate line points
for x, y in zip(service_summary['Service_trans'], service_summary['Transaction Id_trans']):
    ax2.text(x, y, f"{y}", color='red', fontsize=LABEL_FONTSIZE, fontweight=LABEL_FONTWEIGHT, ha='center', va='bottom')
plt.xticks(rotation=90)
lines, labels = ax1.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
ax2.legend(lines + lines2, labels + labels2, loc='upper right')
plt.tight_layout()
plt.show()