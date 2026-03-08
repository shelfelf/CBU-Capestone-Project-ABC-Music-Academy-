#import required libraries
import re
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import timedelta
import os
import warnings
warnings.filterwarnings('ignore')

sns.set_theme(style="whitegrid")
pd.options.display.max_columns = 400
pd.options.display.width = 220

#import all the files
INPUT_XLSX = r"D:\OneDrive - Cape Breton University\Cape Breton University\Semester 4 Subject Material\Capstone Project\Project\Team 1 Project Documents\A_ABC_DATA\ABC_Merged_Data_Final.xlsx"
SHEET_ATT = 'A_attendance_report_FULL'
SHEET_SUB = 'A_subscriptions_report_FULL'
SHEET_INV = 'A_invoice_report_FULL'
SHEET_TRANS = 'A_transactions_report_FULL'

#business config
CONVERSION_WINDOW_DAYS = 180
CONSIDER_FREE_KEYWORDS = ['FREE', 'DEMO', 'TRIAL', 'TRY'] 
ID_MAX_LEN = 6  # truncate IDs to max 6 chars

#Load data
att = pd.read_excel(INPUT_XLSX, sheet_name=SHEET_ATT)
sub = pd.read_excel(INPUT_XLSX, sheet_name=SHEET_SUB)
inv = pd.read_excel(INPUT_XLSX, sheet_name=SHEET_INV)
trans = pd.read_excel(INPUT_XLSX, sheet_name=SHEET_TRANS)

#copy original files
att = att.copy()
sub = sub.copy()
inv = inv.copy()
trans = trans.copy()

#Normalize column names and add suffixes
def normalize_cols(df, suffix):
    df.columns = df.columns.str.upper().str.replace(" ", "_")
    return df.add_suffix(suffix)

att = normalize_cols(att, "_ATT")
sub = normalize_cols(sub, "_SUB")
inv = normalize_cols(inv, "_INV")
trans = normalize_cols(trans, "_TRANS")

#cleaning ID columns
def clean_ids_col(df, col):
    if col in df.columns:
        df[col] = df[col].astype(str).fillna("").str.strip().str.replace(" ", "", regex=False)
        df.loc[df[col] == "", col] = np.nan

id_cols_map = {
    'att': ["ID_ATT", "CLIENT_ID_ATT", "STUDENT_ID_ATT"],
    'sub': ["ID_SUB", "PAYER_ID_SUB", "CLIENT_ID_SUB", "PRIMARY_STAFF_ID_SUB"],
    'inv': ["INVOICE_INV", "INVOICE_PRIMARY_STAFF_ID_INV", "CLIENT_ID_INV", "SUBSCRIPTION_ID_INV", "SUBSCRIPTION_PRIMARY_STAFF_ID_INV"],
    'trans': ["TRANSACTION_ID_TRANS", "PAYER_ID_TRANS", "INVOICE_ID_TRANS", "INVOICE_LINE_ITEM_ID_TRANS"]
}

for key, cols in id_cols_map.items():
    df = locals()[key]
    for c in cols:
        clean_ids_col(df, c)
    locals()[key] = df

#Standardize service text columns
text_cols = [
    ('att', 'SERVICE_ATT'), ('inv', 'SERVICE_INV'), ('sub', 'SERVICE_SUB'), ('trans', 'SERVICE_TRANS')
]
for df_name, col in text_cols:
    df = locals()[df_name]
    if col in df.columns:
        df[col] = df[col].astype(str).fillna("").str.strip().str.upper()
    locals()[df_name] = df

#Normalize date columns
def to_date_col(df, col):
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors='coerce')
        df[col] = df[col].dt.normalize()

#attendance
to_date_col(att, 'START_DATE_ATT')

#invoice
to_date_col(inv, 'CLOSED_DATE_INV')
to_date_col(inv, 'DUE_DATE_INV')

#subscription
to_date_col(sub, 'START_DATE_SUB')
to_date_col(sub, 'END_DATE_SUB')
to_date_col(sub, 'CREATION_DATE_SUB')

#transactions
to_date_col(trans, 'DATE_POSTED_TRANS')
to_date_col(trans, 'TRANSACTION_DUE_DATE_TRANS')
to_date_col(trans, 'INVOICE_CLOSED_DATE_TRANS')

#Identify demo takers (attendance & invoice)
free_pattern = '|'.join([re.escape(k) for k in CONSIDER_FREE_KEYWORDS])

#from attendance
demo_att_full = pd.DataFrame()
if 'SERVICE_ATT' in att.columns:
    demo_att_full = att[att['SERVICE_ATT'].str.contains(free_pattern, na=False, regex=True)].copy()
    demo_att_full['DEMO_SOURCE'] = 'ATTENDANCE'
print(f"Attendance demo rows: {len(demo_att_full)} (unique clients: {demo_att_full['CLIENT_ID_ATT'].nunique()})")

#from invoice
demo_inv_full = pd.DataFrame()
if 'SERVICE_INV' in inv.columns:
    demo_inv_full = inv[inv['SERVICE_INV'].str.contains(free_pattern, na=False, regex=True)].copy()
    demo_inv_full['DEMO_SOURCE'] = 'INVOICE'
print(f"Invoice demo rows: {len(demo_inv_full)} (unique clients: {demo_inv_full['CLIENT_ID_INV'].nunique() if not demo_inv_full.empty else 0})")

#union both demo tables (keep all columns) - create CLIENT_ID column normalized for union
def ensure_client_col(df, client_col_name):
    if client_col_name in df.columns:
        df = df.copy()
        df['CLIENT_ID_UNI'] = df[client_col_name]
    else:
        df = df.copy()
        df['CLIENT_ID_UNI'] = np.nan
    return df

if not demo_att_full.empty:
    df_att_u = ensure_client_col(demo_att_full, 'CLIENT_ID_ATT')
else:
    df_att_u = pd.DataFrame(columns=['CLIENT_ID_UNI'])
if not demo_inv_full.empty:
    df_inv_u = ensure_client_col(demo_inv_full, 'CLIENT_ID_INV')
else:
    df_inv_u = pd.DataFrame(columns=['CLIENT_ID_UNI'])

demo_union_full = pd.concat([df_att_u, df_inv_u], ignore_index=True, sort=False)

#drop rows without client id
demo_union_full = demo_union_full[~demo_union_full['CLIENT_ID_UNI'].isna()].copy()
print(f"Total union demo rows: {len(demo_union_full)}, unique clients: {demo_union_full['CLIENT_ID_UNI'].nunique()}")

#create list of unique demo client IDs
demo_client_ids = set(demo_union_full['CLIENT_ID_UNI'].dropna().unique())

#look up demo IDs in subscriptions to find conversions
#If client id column exists in sub, check membership
if 'CLIENT_ID_SUB' in sub.columns:
    sub['IS_CONVERTED_FLAG'] = sub['CLIENT_ID_SUB'].isin(demo_client_ids)
else:
    sub['IS_CONVERTED_FLAG'] = False

#get unique subscription rows for demo clients (converted)
subs_for_demo = sub[sub['CLIENT_ID_SUB'].isin(demo_client_ids)].copy()
print(f"Subscriptions rows for demo client IDs: {len(subs_for_demo)} (unique clients: {subs_for_demo['CLIENT_ID_SUB'].nunique()})")

#determine converted clients = those demo IDs found in subscriptions
converted_client_ids = set(subs_for_demo['CLIENT_ID_SUB'].dropna().unique())

#create master demo table with converted flag (merge back the converted flag into the union dataset)
demo_clients_df = pd.DataFrame({'CLIENT_ID': list(demo_client_ids)})
demo_clients_df['CONVERTED'] = demo_clients_df['CLIENT_ID'].isin(converted_client_ids)

#add demo sources aggregation - compute if client had attendance/invoice/both
sources = []
for cid in demo_clients_df['CLIENT_ID']:
    in_att = (cid in set(demo_att_full['CLIENT_ID_ATT'].dropna())) if not demo_att_full.empty else False
    in_inv = (cid in set(demo_inv_full['CLIENT_ID_INV'].dropna())) if not demo_inv_full.empty else False
    if in_att and in_inv:
        sources.append('BOTH')
    elif in_att:
        sources.append('ATTENDANCE')
    elif in_inv:
        sources.append('INVOICE')
    else:
        sources.append('UNKNOWN')
demo_clients_df['DEMO_SOURCE'] = sources

print("\nDemo clients summary head/tail:")
print(demo_clients_df.head().to_string(index=False))
print(demo_clients_df.tail().to_string(index=False))

#first demo date per client (from attendance/invoice) and first paid subscription date per client cohort
#derive first demo date per client from demo_union_full using appropriate date columns
#try to pick START_DATE_ATT or CLOSED_DATE_INV as demo date fields if present
demo_dates = []
if not demo_att_full.empty:
    if 'START_DATE_ATT' in demo_att_full.columns:
        demo_dates.append(demo_att_full[['CLIENT_ID_ATT', 'START_DATE_ATT']].rename(columns={'CLIENT_ID_ATT':'CLIENT_ID', 'START_DATE_ATT':'DEMO_DATE'}))
if not demo_inv_full.empty:
    # prefer CLOSED_DATE_INV else DUE_DATE_INV
    inv_date_col = 'CLOSED_DATE_INV' if 'CLOSED_DATE_INV' in demo_inv_full.columns else ('DUE_DATE_INV' if 'DUE_DATE_INV' in demo_inv_full.columns else None)
    if inv_date_col is not None:
        demo_dates.append(demo_inv_full[['CLIENT_ID_INV', inv_date_col]].rename(columns={'CLIENT_ID_INV':'CLIENT_ID', inv_date_col:'DEMO_DATE'}))

if demo_dates:
    demo_dates_df = pd.concat(demo_dates, ignore_index=True, sort=False)
    demo_dates_df = demo_dates_df[~demo_dates_df['CLIENT_ID'].isna()].copy()
    demo_dates_df['DEMO_DATE'] = pd.to_datetime(demo_dates_df['DEMO_DATE'], errors='coerce').dt.normalize()
    first_demo = demo_dates_df.sort_values(['CLIENT_ID','DEMO_DATE']).groupby('CLIENT_ID', as_index=False)['DEMO_DATE'].min().rename(columns={'DEMO_DATE':'FIRST_DEMO_DATE'})
else:
    first_demo = pd.DataFrame(columns=['CLIENT_ID','FIRST_DEMO_DATE'])

print(f"\nFirst demo snapshot: {len(first_demo)} rows (unique clients: {first_demo['CLIENT_ID'].nunique()})")
print(first_demo.head().to_string(index=False))

#first paid subscription per demo client (paid = subscription row where service not containing free keywords)
sub_filtered = sub[sub['CLIENT_ID_SUB'].isin(demo_client_ids)].copy()
if 'SERVICE_SUB' in sub_filtered.columns:
    paid_mask = ~sub_filtered['SERVICE_SUB'].str.contains(free_pattern, na=False, regex=True)
else:
    paid_mask = pd.Series([True] * len(sub_filtered), index=sub_filtered.index)

paid_subs = sub_filtered[paid_mask].copy()
paid_subs['START_DATE_SUB'] = pd.to_datetime(paid_subs.get('START_DATE_SUB', pd.Series(pd.NaT)), errors='coerce').dt.normalize()

first_paid = pd.DataFrame()
if not paid_subs.empty:
    first_paid = paid_subs.sort_values(['CLIENT_ID_SUB','START_DATE_SUB']).groupby('CLIENT_ID_SUB', as_index=False)['START_DATE_SUB'].min().rename(columns={'CLIENT_ID_SUB':'CLIENT_ID', 'START_DATE_SUB':'FIRST_PAID_DATE'})

print(f"First paid snapshot: {len(first_paid)} rows (unique clients: {first_paid['CLIENT_ID'].nunique()})")
print(first_paid.head().to_string(index=False))

#merge into cohort
cohort = pd.DataFrame({'CLIENT_ID': list(demo_client_ids)})
cohort = cohort.merge(first_demo, on='CLIENT_ID', how='left')
cohort = cohort.merge(first_paid, on='CLIENT_ID', how='left')
cohort['DAYS_TO_CONVERT'] = (cohort['FIRST_PAID_DATE'] - cohort['FIRST_DEMO_DATE']).dt.days
cohort['CONVERTED_FLAG'] = cohort['CLIENT_ID'].isin(converted_client_ids)

#define converted within window
cohort['CONVERTED_WITHIN_WINDOW'] = cohort['DAYS_TO_CONVERT'].notna() & (cohort['DAYS_TO_CONVERT'] >= 0) & (cohort['DAYS_TO_CONVERT'] <= CONVERSION_WINDOW_DAYS)

print("\nCohort sample:")
print(cohort.head().to_string(index=False))

#Compute conversion metrics overall & by demo source
total_demo = len(cohort)
converted_within = int(cohort['CONVERTED_WITHIN_WINDOW'].sum())
conversion_rate_pct = (converted_within / total_demo * 100) if total_demo > 0 else 0.0
print(f"\nConversion summary: total_demo={total_demo}, converted_within_window={converted_within}, conversion_rate_pct={conversion_rate_pct:.2f}%")

#by source
cohort = cohort.merge(demo_clients_df[['CLIENT_ID','DEMO_SOURCE']], on='CLIENT_ID', how='left')
source_summary = cohort.groupby('DEMO_SOURCE').agg(total_demo=('CLIENT_ID','count'), converted_within=('CONVERTED_WITHIN_WINDOW','sum')).reset_index()
source_summary['conversion_rate_pct'] = source_summary['converted_within'] / source_summary['total_demo'] * 100
print("\nConversion by demo source:")
print(source_summary.to_string(index=False))

#clean amounts and filter successful positive transactions
#map invoice to client id for transactions to attribute revenue
invoice_to_client = {}
if 'INVOICE_INV' in inv.columns and 'CLIENT_ID_INV' in inv.columns:
    invoice_to_client = inv[['INVOICE_INV','CLIENT_ID_INV']].drop_duplicates().set_index('INVOICE_INV')['CLIENT_ID_INV'].to_dict()

#standardize invoice id
if 'INVOICE_ID_TRANS' in trans.columns:
    trans['INVOICE_ID_TRANS'] = trans['INVOICE_ID_TRANS'].astype(str).replace('nan', np.nan)

#get client id from invoice mapping or fallback to PAYER_ID_TRANS
trans['CLIENT_ID_FROM_INV'] = trans['INVOICE_ID_TRANS'].map(invoice_to_client).astype(object)
if 'PAYER_ID_TRANS' in trans.columns:
    trans.loc[trans['CLIENT_ID_FROM_INV'].isna(), 'CLIENT_ID_FROM_INV'] = trans.loc[trans['CLIENT_ID_FROM_INV'].isna(), 'PAYER_ID_TRANS']

#parse numeric transaction amount
def extract_numeric_amount(s):
    if pd.isna(s):
        return np.nan
    s = str(s)
    m = re.search(r'-?[\d\.,]+', s)
    if not m:
        return np.nan
    amt = m.group(0)
    amt = amt.replace(',', '')
    try:
        return float(amt)
    except:
        return np.nan

#prefer TRANSACTION_AMOUNT_TRANS or INVOICE_LINE_ITEM_AMOUNT_TRANS if present
if 'TRANSACTION_AMOUNT_TRANS' in trans.columns:
    trans['TRANSACTION_AMOUNT_NUM'] = trans['TRANSACTION_AMOUNT_TRANS'].apply(extract_numeric_amount)
elif 'INVOICE_LINE_ITEM_AMOUNT_TRANS' in trans.columns:
    trans['TRANSACTION_AMOUNT_NUM'] = trans['INVOICE_LINE_ITEM_AMOUNT_TRANS'].apply(extract_numeric_amount)
else:
    trans['TRANSACTION_AMOUNT_NUM'] = np.nan

#keep only successful transactions and positive amounts
if 'TRANSACTION_STATUS_TRANS' in trans.columns:
    trans_success = trans[(trans['TRANSACTION_STATUS_TRANS'].astype(str).str.upper() == 'SUCCESSFUL') & (trans['TRANSACTION_AMOUNT_NUM'] > 0)].copy()
else:
    trans_success = trans[trans['TRANSACTION_AMOUNT_NUM'] > 0].copy()

#restrict transaction rows to those that map to demo clients
trans_demo = trans_success[trans_success['CLIENT_ID_FROM_INV'].isin(demo_client_ids)].copy()
trans_demo.rename(columns={'CLIENT_ID_FROM_INV':'CLIENT_ID'}, inplace=True)
print(f"\nTransactions (successful + positive) mapping to demo clients: {len(trans_demo)} rows")

#transactions for converted clients only
trans_converted = trans_demo[trans_demo['CLIENT_ID'].isin(converted_client_ids)].copy()
print(f"Transactions for converted clients: {len(trans_converted)} rows")

#total revenue (all transactions for converted clients)
total_revenue_converted = trans_converted['TRANSACTION_AMOUNT_NUM'].sum(min_count=1)
total_revenue_converted = float(total_revenue_converted) if not pd.isna(total_revenue_converted) else 0.0
num_converted_clients = len(converted_client_ids)
avg_rev_per_converted = total_revenue_converted / num_converted_clients if num_converted_clients > 0 else 0.0

print(f"\nRevenue (converted clients): total=${total_revenue_converted:,.2f}, n_clients={num_converted_clients}, avg_rev/client=${avg_rev_per_converted:,.2f}")

#Attribute revenue AFTER conversion date
#For each converted client, sum transactions where transaction date >= FIRST_PAID_DATE (or >= FIRST_PAID_DATE - small tolerance)
if not cohort[cohort['CONVERTED_WITHIN_WINDOW']].empty:
    #join transaction with cohort FIRST_PAID_DATE
    trans_conv_join = trans_converted.merge(cohort[['CLIENT_ID','FIRST_PAID_DATE']], on='CLIENT_ID', how='left')
    trans_conv_join['DATE_POSTED_TRANS'] = pd.to_datetime(trans_conv_join.get('DATE_POSTED_TRANS', pd.NaT), errors='coerce')
    #consider revenue on or after FIRST_PAID_DATE
    trans_conv_post = trans_conv_join[trans_conv_join['DATE_POSTED_TRANS'] >= trans_conv_join['FIRST_PAID_DATE']]
    revenue_after_conversion = trans_conv_post['TRANSACTION_AMOUNT_NUM'].sum(min_count=1)
    revenue_after_conversion = float(revenue_after_conversion) if not pd.isna(revenue_after_conversion) else 0.0
else:
    revenue_after_conversion = 0.0

print(f"Revenue from converted clients (transactions on/after FIRST_PAID_DATE): ${revenue_after_conversion:,.2f}")

#Save revenue per client
rev_by_client = trans_converted.groupby('CLIENT_ID')['TRANSACTION_AMOUNT_NUM'].sum().reset_index().rename(columns={'TRANSACTION_AMOUNT_NUM':'TOTAL_REVENUE'})
rev_by_client_sorted = rev_by_client.sort_values('TOTAL_REVENUE', ascending=False)

#Identify Service patterns (conversion & revenue)
#Service conversion (from subscription table): which SERVICE_SUB values appear for converted clients
if 'SERVICE_SUB' in sub.columns:
    subs_converted = sub[sub['CLIENT_ID_SUB'].isin(converted_client_ids)].copy()
    svc_conv = subs_converted.groupby('SERVICE_SUB').agg(conversions=('CLIENT_ID_SUB','nunique')).reset_index().sort_values('conversions', ascending=False)
    #total revenue by service (from transactions linked to subscription service when possible; else use transaction SERVICE_TRANS)
    #try to map transactions to service via SERVICE_TRANS or INVOICE_LINE_ITEM_DESCRIPTION
    trans_conv_srv = trans_converted.copy()
    #priority: SERVICE_TRANS, else use INV/LINE ITEM DESC if available
    trans_conv_srv['SERVICE_USED'] = trans_conv_srv.get('SERVICE_TRANS', '').astype(str).fillna('')
    #aggregate revenue by SERVICE_USED
    rev_by_service = trans_conv_srv.groupby('SERVICE_USED')['TRANSACTION_AMOUNT_NUM'].sum().reset_index().rename(columns={'TRANSACTION_AMOUNT_NUM':'TOTAL_REVENUE'}).sort_values('TOTAL_REVENUE', ascending=False)
else:
    svc_conv = pd.DataFrame(columns=['SERVICE_SUB','conversions'])
    rev_by_service = pd.DataFrame(columns=['SERVICE_USED','TOTAL_REVENUE'])

#Print heads
print("\nTop services by conversions (subscription table):")
print(svc_conv.head().to_string(index=False))
print("\nTop services by revenue (transactions mapping):")
print(rev_by_service.head().to_string(index=False))

#Location-based analysis
location_col_att = 'LOCATION_ATT' if 'LOCATION_ATT' in att.columns else None
location_col_sub = 'LOCATION_SUB' if 'LOCATION_SUB' in sub.columns else None

#conversions by location
location_rows = []
for cid in demo_clients_df['CLIENT_ID']:
    # find subscription location if converted
    loc = np.nan
    if cid in converted_client_ids and location_col_sub is not None:
        locs = sub[sub['CLIENT_ID_SUB'] == cid][location_col_sub].dropna().unique()
        if len(locs) > 0:
            loc = locs[0]
    if pd.isna(loc) and location_col_att is not None:
        locs = att[att['CLIENT_ID_ATT'] == cid][location_col_att].dropna().unique()
        if len(locs) > 0:
            loc = locs[0]
    location_rows.append({'CLIENT_ID':cid, 'LOCATION':loc, 'CONVERTED': cid in converted_client_ids})

location_df = pd.DataFrame(location_rows)
location_summary = location_df.groupby('LOCATION').agg(total_demo=('CLIENT_ID','count'), converted=('CONVERTED','sum')).reset_index()
location_summary['conversion_rate_pct'] = location_summary['converted'] / location_summary['total_demo'] * 100
print("\nLocation summary (top rows):")
print(location_summary.sort_values('total_demo', ascending=False).head().to_string(index=False))

#revenue by location: map transactions to client's location (from location_df)
trans_loc = trans_converted.merge(location_df[['CLIENT_ID','LOCATION']], on='CLIENT_ID', how='left')
rev_by_location = trans_loc.groupby('LOCATION')['TRANSACTION_AMOUNT_NUM'].sum().reset_index().rename(columns={'TRANSACTION_AMOUNT_NUM':'TOTAL_REVENUE'}).sort_values('TOTAL_REVENUE', ascending=False)

#Retention / Client Status
if 'CLIENT_STATUS_SUB' in sub.columns:
    status_for_converted = sub[sub['CLIENT_ID_SUB'].isin(converted_client_ids)][['CLIENT_ID_SUB','CLIENT_STATUS_SUB']].drop_duplicates(subset=['CLIENT_ID_SUB']).rename(columns={'CLIENT_ID_SUB':'CLIENT_ID'})
    #compute retention metrics
    status_summary = status_for_converted.groupby('CLIENT_STATUS_SUB').agg(num_clients=('CLIENT_ID','nunique')).reset_index()
    print("\nClient status summary for converted clients:")
    print(status_summary.to_string(index=False))

#Time-to-convert clusters & histograms for 0-3,4-7,>7 days
def convert_cluster(days):
    if pd.isna(days):
        return 'NO_CONVERSION'
    if days <= 3:
        return '0-3'
    elif 4 <= days <= 7:
        return '4-7'
    elif days > 7:
        return '8+'
    else:
        return 'NO_CONVERSION'

cohort['CONVERSION_CLUSTER'] = cohort['DAYS_TO_CONVERT'].apply(convert_cluster)
cluster_summary = cohort.groupby('CONVERSION_CLUSTER').agg(count=('CLIENT_ID','count'), converted_within=('CONVERTED_WITHIN_WINDOW','sum')).reset_index()
print("\nConversion clusters:")
print(cluster_summary.to_string(index=False))

#Visualizations
#label style
LABEL_FONTSIZE = 11
LABEL_FONTWEIGHT = 'bold'
LABEL_COLOR = 'black'

#plotting annotation
def annotate_bars(ax, fmt=None, fontsize=LABEL_FONTSIZE, fontweight=LABEL_FONTWEIGHT, color=LABEL_COLOR, offset_rel=0.02):
    patches = [p for p in ax.patches if np.isfinite(p.get_height())]
    if not patches:
        return
    maxh = max([p.get_height() for p in patches]) if patches else 0
    for p in patches:
        x = p.get_x() + p.get_width() / 2
        h = p.get_height()
        offset = offset_rel * (maxh if maxh != 0 else 1)
        if fmt:
            label = fmt.format(h)
        else:
            label = f"{int(h)}"
        ax.text(x, h + offset, label, ha='center', va='bottom', fontsize=fontsize, fontweight=fontweight, color=color)

def annotate_percent_bars(ax, fontsize=LABEL_FONTSIZE, fontweight=LABEL_FONTWEIGHT, color=LABEL_COLOR, offset_rel=0.02):
    patches = [p for p in ax.patches if np.isfinite(p.get_height())]
    if not patches:
        return
    maxh = max([p.get_height() for p in patches]) if patches else 0
    for p in patches:
        x = p.get_x() + p.get_width() / 2
        h = p.get_height()
        offset = offset_rel * (maxh if maxh != 0 else 1)
        ax.text(x, h + offset, f"{h:.1f}%", ha='center', va='bottom', fontsize=fontsize, fontweight=fontweight, color=color)

#annotate histogram bars
def annotate_hist_patches(ax, fmt=None, fontsize=LABEL_FONTSIZE, fontweight=LABEL_FONTWEIGHT, color=LABEL_COLOR, offset_rel=0.02):
    patches = [p for p in ax.patches]
    if not patches:
        return
    maxh = max([p.get_height() for p in patches]) if patches else 0
    for p in patches:
        x = p.get_x() + p.get_width() / 2
        h = p.get_height()
        label = (fmt.format(int(h)) if fmt else f"{int(h)}")
        offset = offset_rel * (maxh if maxh != 0 else 1)
        ax.text(x, h + offset, label, ha='center', va='bottom', fontsize=fontsize, fontweight=fontweight, color=color)

#Utility for percent axis formatting (keeps numeric values but we annotate)
plt.rcParams.update({'figure.max_open_warning': 0})

#Overall conversion bar
plt.figure(figsize=(8,5))
ax = sns.barplot(x=['Converted', 'Not Converted'], y=[converted_within, total_demo - converted_within], palette='Blues')
plt.title(f"Overall conversion (within {CONVERSION_WINDOW_DAYS} days)")
plt.ylabel("Number of clients")
annotate_bars(ax, fmt="{:.0f}")
plt.tight_layout()
plt.show()

#Conversion by demo source
plt.figure(figsize=(10,5))
ax = sns.barplot(data=source_summary, x='DEMO_SOURCE', y='conversion_rate_pct', color='skyblue')
plt.title("Conversion rate by demo source (Attendance/Invoice/Both)")
plt.ylabel("Conversion rate (%)")
plt.xlabel("Demo source")
annotate_percent_bars(ax)
plt.tight_layout()
plt.show()

#Days-to-convert histogram with mean/median
plt.figure(figsize=(10,5))
ax = sns.histplot(cohort[cohort['DAYS_TO_CONVERT'].notna()]['DAYS_TO_CONVERT'], bins=30, kde=True, color='slateblue')
plt.title("Distribution of days to convert (first demo -> first paid subscription)")
plt.xlabel("Days to convert")
plt.ylabel("Number of clients")
annotate_hist_patches(ax, fmt="{:.0f}")
mean_val = cohort['DAYS_TO_CONVERT'].mean()
median_val = cohort['DAYS_TO_CONVERT'].median()
ax.axvline(mean_val, color='red', linestyle='--', label=f"Mean: {mean_val:.1f}")
ax.axvline(median_val, color='green', linestyle=':', label=f"Median: {median_val:.1f}")
ax.legend()
plt.tight_layout()
plt.show()

#Conversion cluster bar
plt.figure(figsize=(8,5))
cluster_plot = cluster_summary.sort_values('count', ascending=False)
ax = sns.barplot(data=cluster_plot, x='CONVERSION_CLUSTER', y='count', order=['0-3','4-7','8+','NO_CONVERSION'])
plt.title("Conversion clusters (by days to convert)")
plt.xlabel("Cluster")
plt.ylabel("Number of clients")
annotate_bars(ax, fmt="{:.0f}")
plt.tight_layout()
plt.show()

#Top 10 Converted Client by Revenue
rev_by_client = trans_converted.groupby('CLIENT_ID')['TRANSACTION_AMOUNT_NUM'].sum().reset_index().rename(columns={'TRANSACTION_AMOUNT_NUM':'TOTAL_REVENUE'})
rev_by_client_sorted = rev_by_client.sort_values('TOTAL_REVENUE', ascending=False)
top_n = 10
plt.figure(figsize=(12,6))
ax = sns.barplot(data=rev_by_client_sorted.head(top_n), x='CLIENT_ID', y='TOTAL_REVENUE')
plt.title(f"Top {top_n} Converted Clients by Revenue")
plt.ylabel("Total Revenue")
plt.xlabel("Client ID")
plt.xticks(rotation=45, ha='right')

#annotate with currency format
for p in ax.patches:
    h = p.get_height()
    x = p.get_x() + p.get_width()/2
    ax.text(x, h + 0.01 * (rev_by_client_sorted['TOTAL_REVENUE'].max() if not rev_by_client_sorted.empty else 1),
    f"${h:,.2f}", ha='center', fontsize=LABEL_FONTSIZE, fontweight=LABEL_FONTWEIGHT, color=LABEL_COLOR)
plt.tight_layout()
plt.show()

#Top services by conversion
plt.figure(figsize=(12,6))
topn = min(10, len(svc_conv))
ax = sns.barplot(data=svc_conv.head(topn), x='SERVICE_SUB', y='conversions')
plt.title(f"Top {topn} Service Types by Conversions (subscriptions)")
plt.xlabel("Service")
plt.ylabel("Conversions")
plt.xticks(rotation=45, ha='right')
annotate_bars(ax)
plt.tight_layout()
plt.show()

#Top 4 services by revenue
plt.figure(figsize=(12,6))
topn = min(10, len(rev_by_service))
ax = sns.barplot(data=rev_by_service.head(topn), x='SERVICE_USED', y='TOTAL_REVENUE')
plt.title(f"Top {topn} Services by Revenue (converted clients)")
plt.xlabel("Service (transaction-level)")
plt.ylabel("Revenue ($)")
plt.xticks(rotation=45, ha='right')

#annotate currency
for p in ax.patches:
    h = p.get_height()
    x = p.get_x() + p.get_width() / 2
    ax.text(x, h + 0.01 * rev_by_service['TOTAL_REVENUE'].max(), f"${h:,.2f}", ha='center', fontsize=LABEL_FONTSIZE, fontweight=LABEL_FONTWEIGHT)
plt.tight_layout()
plt.show()

#Top Location by conversion volume
plt.figure(figsize=(12,6))
loc_plot = location_summary.sort_values('total_demo', ascending=False).head(12)
ax = sns.barplot(data=loc_plot, x='LOCATION', y='total_demo')
plt.title("Top locations by demo volume")
plt.xlabel("Location")
plt.ylabel("Demo count")
plt.xticks(rotation=45, ha='right')
annotate_bars(ax)
plt.tight_layout()
plt.show()

#Top Location by revenue
plt.figure(figsize=(12,6))
loc_rev_plot = rev_by_location.head(12)
ax = sns.barplot(data=loc_rev_plot, x='LOCATION', y='TOTAL_REVENUE')
plt.title("Top locations by revenue (converted clients)")
plt.xlabel("Location")
plt.ylabel("Revenue ($)")
plt.xticks(rotation=45, ha='right')
for p in ax.patches:
    h = p.get_height()
    x = p.get_x() + p.get_width()/2
    ax.text(x, h + 0.01 * rev_by_location['TOTAL_REVENUE'].max(), f"${h:,.2f}", ha='center', fontsize=LABEL_FONTSIZE, fontweight=LABEL_FONTWEIGHT)
plt.tight_layout()
plt.show()

#Retention chart by Member and Member Lost
plt.figure(figsize=(8,5))
ax = sns.barplot(data=status_summary, x='CLIENT_STATUS_SUB', y='num_clients')
plt.title("Converted clients by status (Member vs Member Lost)")
plt.xlabel("Client status")
plt.ylabel("Number of clients")
annotate_bars(ax)
plt.tight_layout()
plt.show()

#Demo weekday or month to conversion rate
cohort['DEMO_WEEKDAY'] = cohort['FIRST_DEMO_DATE'].dt.day_name()
cohort['DEMO_MONTH'] = cohort['FIRST_DEMO_DATE'].dt.to_period('M').astype(str)
cohort['CONVERSION_DATE'] = cohort['FIRST_PAID_DATE']
cohort['CONVERSION_WEEKDAY'] = cohort['CONVERSION_DATE'].dt.day_name()
cohort['CONVERSION_MONTH'] = cohort['CONVERSION_DATE'].dt.to_period('M').astype(str)

weekday_order = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
cohort['CONVERTED'] = cohort['CLIENT_ID'].isin(converted_client_ids)
weekday_summary = cohort.groupby('DEMO_WEEKDAY')['CONVERTED'].agg(['count','sum']).reindex(weekday_order).reset_index()
weekday_summary['conversion_rate_pct'] = weekday_summary['sum'] / weekday_summary['count'] * 100

plt.figure(figsize=(10,5))
ax = sns.barplot(data=weekday_summary, x='DEMO_WEEKDAY', y='conversion_rate_pct')
plt.title("Conversion rate by demo weekday")
plt.ylabel("Conversion rate (%)")
plt.xlabel("Demo weekday")

#annotate percentages
for p in ax.patches:
    h = p.get_height()
    x = p.get_x() + p.get_width() / 2
    ax.text(x, h + 0.01 * max(weekday_summary['conversion_rate_pct'].max(), 1), f"{h:.1f}%", ha='center', fontsize=LABEL_FONTSIZE, fontweight=LABEL_FONTWEIGHT, color=LABEL_COLOR)
plt.tight_layout()
plt.show()
print("\nConversion rate by demo weekday:")
print(weekday_summary.to_string(index=False))

#conversion rate by demo month
month_summary = cohort.groupby('DEMO_MONTH')['CONVERTED'].agg(['count','sum']).reset_index().sort_values('DEMO_MONTH')
month_summary['conversion_rate_pct'] = month_summary['sum'] / month_summary['count'] * 100

plt.figure(figsize=(12,5))
ax = sns.barplot(data=month_summary, x='DEMO_MONTH', y='conversion_rate_pct')
plt.title("Conversion rate by demo month")
plt.ylabel("Conversion rate (%)")
plt.xlabel("Demo month (YYYY-MM)")
plt.xticks(rotation=45)
for p in ax.patches:
    h = p.get_height()
    x = p.get_x() + p.get_width() / 2
    ax.text(x, h + 0.01 * max(month_summary['conversion_rate_pct'].max(), 1), f"{h:.1f}%", ha='center', fontsize=LABEL_FONTSIZE, fontweight=LABEL_FONTWEIGHT, color=LABEL_COLOR)
plt.tight_layout()
plt.show()
print("\nConversion rate by demo month:")
print(month_summary.to_string(index=False))

#Final summary tables to save
OUTPUT_DIR = r"D:\OneDrive - Cape Breton University\Cape Breton University\Semester 4 Subject Material\Capstone Project\Project\Team 1 Project Documents\A_ABC_DATA\outputs"
cohort.to_csv(os.path.join(OUTPUT_DIR, "cohort_summary_final.csv"), index=False)
demo_union_full.to_csv(os.path.join(OUTPUT_DIR, "demo_union_full.xlsx"), index=False)
demo_clients_df.to_csv(os.path.join(OUTPUT_DIR, "demo_clients_summary.csv"), index=False)
location_summary.to_csv(os.path.join(OUTPUT_DIR, "location_conversion_summary_final.csv"), index=False)
rev_by_location.to_csv(os.path.join(OUTPUT_DIR, "rev_by_location_final.csv"), index=False)
rev_by_client_sorted.to_csv(os.path.join(OUTPUT_DIR, "rev_by_client_converted_final.csv"), index=False)
rev_by_service.to_csv(os.path.join(OUTPUT_DIR, "rev_by_service_final.csv"), index=False)
status_for_converted.to_csv(os.path.join(OUTPUT_DIR, "status_for_converted_clients.csv"), index=False)
svc_conv.to_csv(os.path.join(OUTPUT_DIR, "service_conversion_counts_final.csv"), index=False)
trans_demo.to_csv(os.path.join(OUTPUT_DIR, "trans_demo_success.csv"), index=False)
status_summary.to_csv(os.path.join(OUTPUT_DIR, "status_summary_final.csv"), index=False)
print("\nSaved summary outputs to:", OUTPUT_DIR)

# Print head/tail of the primary outputs
print("\n--- Cohort (head/tail) ---")
print(cohort.head().to_string(index=False))
print(cohort.tail().to_string(index=False))

print("\n--- Revenue by client (top rows) ---")
print(rev_by_client_sorted.head(20).to_string(index=False))

print("\n--- Service conversion (head) ---")
print(svc_conv.head(20).to_string(index=False))

print("\n--- Location summary (head) ---")
print(location_summary.sort_values('total_demo', ascending=False).head(20).to_string(index=False))