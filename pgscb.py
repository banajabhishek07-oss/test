## Importing Libraries
import pandas as pd
import polars as pl
from datetime import date
from datetime import datetime, timedelta
import psycopg2
import pygsheets
import io
import gspread
import yaml
import numpy as np
from pyathena import connect
import os
import boto3
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import re

gs = pygsheets.authorize(
    service_file='/Users/banajabhishekchanappa/Downloads/data/service_account.json')

config = yaml.safe_load(open("/Users/banajabhishekchanappa/Tasks/IVRS_ai_appointments/config.yml", "r"))


athena_conn = connect(s3_staging_dir= config['AWS_athena_conn']['s3_staging_dir'],
               region_name= config['AWS_athena_conn']['region_name'],
               aws_access_key_id= config['AWS_athena_conn']['access_key_ID'],
               aws_secret_access_key= config['AWS_athena_conn']['secret_access_key']
               )

def create_redshift_connection(config_file, database):
    redshift = psycopg2.connect(
        database= database,
        user= config_file['redshift_conn']['user'],
        password= config_file['redshift_conn']['password'],
        host= config_file['redshift_conn']['host'],
        port= config_file['redshift_conn']['port']
    )
    return redshift

conn_pw = create_redshift_connection(config, 'practowarehouse')
conn_pii = create_redshift_connection(config, 'practowarehouse_pii')
olap_db = create_redshift_connection(config, 'olapwarehouse')
print('✓ Database connections established')

gross_data=pd.read_csv('/Users/banajabhishekchanappa/Downloads/data/gross_txns_till2024.csv')
gross_data['event_date'] = pd.to_datetime(gross_data['event_date'])

# # Filter data till 30 Nov 2025 (inclusive)
# gross_data = gross_data[
#     gross_data['event_date'] <= '2025-11-30'
# ]
raw_data = gross_data
# print('mac of data',gross_data['event_date'].max())

# assured_raw = pd.read_csv(dest, low_memory=False)
assured_raw = pd.read_csv('/Users/banajabhishekchanappa/Downloads/local_billing_raw (1).csv')
def load_google_sheet_with_duplicate_headers(sheet):
    values = sheet.get_all_values()
    header = values[0]
    rows = values[1:]

    cleaned_header = []
    count = {}
    for h in header:
        name = h.strip() if h.strip() else "column"
        if name in count:
            count[name] += 1
            name = f"{name}_{count[name]}"
        else:
            count[name] = 1
        cleaned_header.append(name)

    return pd.DataFrame(rows, columns=cleaned_header)

sh = gs.open_by_key("1bWyJjZ5HWyLb6fsvWm9vTU_2X6XJSl-glscXZDfDh9s")
ws = sh.worksheet("title","Master")

clinic_master = load_google_sheet_with_duplicate_headers(ws)
clinic_master = clinic_master[["ray_practice_id", "practice_id", "speciality"]].drop_duplicates()
clinic_master = clinic_master[clinic_master["speciality"] != "Diabetes"]
clinic_master.rename(columns={"speciality": "keyword"}, inplace=True)

clinic_master["ray_practice_id"] = clinic_master["ray_practice_id"].astype(str)
clinic_master["practice_id"] = clinic_master["practice_id"].astype(str)
clinic_master["keyword"] = clinic_master["keyword"].astype('category')

excluded_ids = [
    "7a1dcf97-e375-45cb-b16e-edd3ff050afc","98a6cef9-8669-4929-a77b-e8cd66114c73",
    "27762ec5-e84b-11ee-9276-0623476dd86d","17caae2c-4c07-4cec-995e-628444f297e9",
    "5e225516-fe56-4d81-a84b-1408f6a17a6b","31690a56-9f18-4321-9abb-cde4e72267f8",
    "3d3b015a-a16c-4f0b-8f2a-91d6003337dd","6ce84a5a-542e-469f-b6c5-90678f32395f",
    "27df7f31-dcd7-45b0-aeaa-717630220cde","cdae2bb9-68e3-459b-b092-c3a2762f589d",
    "91fc29a3-3fca-4224-94e4-103de0dc525f","4c1e0345-7a52-4939-abfe-590e9f257b33",
    "582cb302-bedc-4cce-ae15-da3cea79d8f8","c0cd41e2-1238-47d6-b935-44f8d2a31023"
]

assured = assured_raw[
    ~assured_raw["prospect_id"].isin(excluded_ids)
][[
    "prospect_id","prospect_activity_id","uhid","practice_id","doctor_id",
    "ipd_invoice_date","opd_invoice_date","diagnostics_invoice_date",
    "pharmacy_invoice_date","cost_per_walkin","ipd_practo_share",
    "dx_practo_share","rx_practo_share",
    "original_opd_date","original_ipd_date","original_diagnostics_date",
    "refund_date","ip_refund","op_refund","dx_refund"
]].copy()

assured.rename(columns={
    "cost_per_walkin":"opd_share",
    "ipd_practo_share":"ipd_share",
    "dx_practo_share":"diag_share",
    "rx_practo_share":"pharma_share",
    "ipd_invoice_date":"ipd_date",
    "opd_invoice_date":"opd_date",
    "diagnostics_invoice_date":"diag_date",
    "pharmacy_invoice_date":"pharma_date"
}, inplace=True)

assured.fillna({"ip_refund":0, "op_refund":0, "dx_refund":0}, inplace=True)
assured["assured_refund_practo_share"] = assured["ip_refund"] + assured["op_refund"] + assured["dx_refund"]
assured.drop(["ip_refund","op_refund","dx_refund"], axis=1, inplace=True)

assured = assured.drop_duplicates()

for col in ["ipd_date","opd_date","diag_date","pharma_date",
            "refund_date","original_opd_date","original_ipd_date","original_diagnostics_date"]:
    assured[col] = pd.to_datetime(assured[col], errors="coerce")

# ✅ FIX: Use range() instead of index to avoid contiguity issues
assured["bill_id"] = "bill" + (pd.Series(range(1, len(assured) + 1)).astype(str))

assured.loc[(assured.opd_share>0) & assured.opd_date.isna(), "opd_date"] = assured["original_opd_date"]
assured.loc[(assured.ipd_share>0) & assured.ipd_date.isna(), "ipd_date"] = assured["original_ipd_date"]
assured.loc[(assured.diag_share>0) & assured.diag_date.isna(), "diag_date"] = assured["original_diagnostics_date"]

assured.drop(["original_opd_date","original_ipd_date","original_diagnostics_date"], axis=1, inplace=True)

def build_block(df, date_col, share_col, bill_type):
    tmp = df[df[share_col] > 0][[
        "bill_id","prospect_id","uhid","practice_id","doctor_id",date_col,share_col
    ]]
    tmp = tmp.rename(columns={date_col:"bill_date", share_col:"practo_share"})
    tmp["type"] = bill_type
    return tmp

opd = build_block(assured, "opd_date", "opd_share", "opd")
ipd = build_block(assured, "ipd_date", "ipd_share", "ipd")
diag = build_block(assured, "diag_date", "diag_share", "diag")
pharma = build_block(assured, "pharma_date", "pharma_share", "pharma")

refund = assured[assured.assured_refund_practo_share > 0][[
    "bill_id","prospect_id","uhid","practice_id","doctor_id","refund_date","assured_refund_practo_share"
]].copy()
refund["practo_share"] = -refund["assured_refund_practo_share"]
refund = refund.rename(columns={"refund_date":"bill_date"})
refund["type"] = "refund"

assured_final = pd.concat([opd, ipd, diag, pharma, refund], ignore_index=True)
assured_final = assured_final[assured_final.bill_date.notna()]

assured_bills_df = (
    assured_final
    .assign(month=lambda x: x.bill_date.astype(str).str[:7])
    .groupby(["practice_id","doctor_id","month"], as_index=False)
    .agg({"practo_share":"sum"})
)

# del assured_final
# gc_module.collect()

dental_ids = clinic_master.ray_practice_id[
    clinic_master.keyword == "General Dentistry"
].astype(str).tolist()

dermat_ids = clinic_master.ray_practice_id[
    clinic_master.keyword == "General Dermatology"
].astype(str).tolist()

# SQL queries EXACTLY LIKE R
dental_df = pd.read_sql("""
select p.ray_practice_id, p.created_at_date as bill_date, 
       sum(amount_paid) as practo_share
from ba_team_olap.practo_one_payments p
join ba_team_olap.practo_one_appointments a
on p.linked_ray_appointment_id = a.ray_appointment_id
where p.cancelled_flag=0 and p.refund_flag=0 
  and a.user_source='practo'
group by 1,2
""", olap_db)

dermat_df = pd.read_sql("""
select p.ray_practice_id, payment_created_date as bill_date, 
       sum(treatment_final_amount) as practo_share
from ba_team_olap.practo_one_payments_invoices p
join ba_team_olap.practo_one_appointments a
on p.linked_ray_appointment_id = a.ray_appointment_id
where is_payment_cancelled=0 
  and treatment_type='treatment' 
  and a.user_source='practo'
group by 1,2
""", olap_db)

dental_df["ray_practice_id"] = dental_df["ray_practice_id"].astype(str)
dermat_df["ray_practice_id"] = dermat_df["ray_practice_id"].astype(str)

dental_df = dental_df[dental_df["ray_practice_id"].isin(dental_ids)]
dermat_df = dermat_df[dermat_df["ray_practice_id"].isin(dermat_ids)]

clinic_bills_copy = pd.concat([dental_df, dermat_df], ignore_index=True)

clinic_bills_copy["ray_practice_id"] = clinic_bills_copy["ray_practice_id"].astype(str)
clinic_master["ray_practice_id"] = clinic_master["ray_practice_id"].astype(str)

clinic_bills_copy = clinic_bills_copy.merge(clinic_master, on="ray_practice_id", how="left")
clinic_bills_copy = clinic_bills_copy[clinic_bills_copy.practice_id.notna()]

clinic_bills_copy["practo_share"] = clinic_bills_copy["practo_share"].astype(float) * 0.38
clinic_bills_copy["bill_date"] = pd.to_datetime(clinic_bills_copy["bill_date"], errors="coerce")

clinic_bills_df = (
    clinic_bills_copy
    .assign(month=lambda x: x.bill_date.astype(str).str[:7])
    .groupby(["practice_id","month"], as_index=False)
    .agg({"practo_share":"sum"})
)

print("✓ Bills data ready")

only_corporate_customers = (raw_data.groupby('mobile')['type'].nunique().reset_index())

only_corporate_customers = (raw_data.groupby('mobile')['type'].apply(lambda x: set(x) == {'corporate_plan_cx'}))

raw_data = raw_data[~raw_data['mobile'].isin(only_corporate_customers[only_corporate_customers].index)]
raw_data=raw_data[raw_data['is_monetisable']==1]
raw_data.loc[raw_data['type'] == 'retail_plan_cx', 'practo_share'] = 137
raw_data['program_name'] = raw_data['program_name'].str.lower()

# Clean up intermediate variable
# del only_corporate_customers
# gc_module.collect()

mask_assured = raw_data['program_name'] == 'assured'
mask_poc = raw_data['program_name'] == 'poc'
df_others = raw_data[~(mask_assured | mask_poc)].copy()

raw_data['event_date'] = pd.to_datetime(raw_data['event_date'])
raw_data['year_month'] = raw_data['event_date'].dt.to_period('M').astype(str)
df_assured = raw_data[raw_data['program_name'].str.lower() == 'assured']
df_poc = raw_data[raw_data['program_name'].str.lower() == 'poc']

df_assured_monetised = df_assured[df_assured['is_monetised'] == 1]
df_poc_monetised = df_poc[df_poc['is_monetised'] == 1]

# Clean up intermediate variables
# del mask_assured, mask_poc
# gc_module.collect()

assured_txn_summary = (df_assured_monetised.groupby(['practice_id', 'relation_id', 'year_month']).size().reset_index(name='assured_txn_count'))

poc_txn_summary = (df_poc_monetised.groupby(['practice_id', 'year_month']).size().reset_index(name='poc_txn_count'))

doc_spec_query=pd.read_sql_query( '''with s as (
    SELECT DISTINCT 
        provider_id, ss.speciality,
        COALESCE(rmk.name, ss.speciality) AS keyword
    FROM (
        SELECT 
            provider_id,
            speciality
        FROM (
            SELECT DISTINCT 
                pp.id AS provider_id,
                mps.name AS speciality,
                sp.priority,
                DENSE_RANK() OVER (PARTITION BY pp.id ORDER BY sp.priority) AS final_spec
            FROM 
                titan_pii.providers_published pp
                JOIN titan_pii.provider_subspecializations_published psp ON pp.id = psp.provider_id
                JOIN titan_pii.master_provider_subspecialities mpss ON psp.subspecialization_id = mpss.id
                JOIN titan_pii.master_provider_specialities mps ON mpss.speciality_id = mps.id
                JOIN (
                    SELECT 
                        mps.name AS speciality,
                        ROW_NUMBER() OVER (ORDER BY COUNT(DISTINCT pp.id) DESC) AS priority
                    FROM 
                        titan_pii.providers_published pp
                        JOIN titan_pii.provider_subspecializations_published psp ON pp.id = psp.provider_id
                        JOIN titan_pii.master_provider_subspecialities mpss ON psp.subspecialization_id = mpss.id
                        JOIN titan_pii.master_provider_specialities mps ON mpss.speciality_id = mps.id
                    WHERE
                        psp.status = 'LIVE'
                        AND mpss.status = 'LIVE'
                        AND mps.status = 'LIVE'
                    GROUP BY 
                        1
                    ORDER BY 
                        2
                ) sp ON mps.name = sp.speciality
                WHERE
                    psp.status = 'LIVE'
                    AND mpss.status = 'LIVE'
                    AND mps.status = 'LIVE'
            )
        WHERE 
            final_spec = 1
        ) ss
        LEFT JOIN reach_campaign_pii.master_specialities rms ON rms.name = ss.speciality
        LEFT JOIN reach_campaign_pii.master_keywords rmk ON rmk.id = rms.keyword_id
)

select
distinct pp.id as doctor_id,
mc.name as city,
mc.country_id as country,
pp.slug as doctor_slug, 
keyword,
s.speciality as speciality
from
titan_pii.providers_published pp 
JOIN titan_pii.master_cities mc ON pp.city_id = mc.id
left join s on s.provider_id = pp.id
where mc.country_id in (1,8,15)
''',conn_pii)
doc_spec_query['speciality'] = doc_spec_query['speciality'].fillna('Others')

doc_spec_query=doc_spec_query[['doctor_id','city','country','speciality']]
relation_ids=pd.read_sql_query('''select id as relation_id,
establishment_id as practice_id,
provider_id as doctor_id
from titan_pii.relations_published
where status='LIVE' ''',conn_pii)
relation__spec=relation_ids.merge(doc_spec_query,on='doctor_id',how='left')

spec_counts = relation__spec.groupby('practice_id')['speciality'].nunique().reset_index()
spec_counts.columns = ['practice_id', 'spec_count']

relation_spec_practice_level = relation__spec.merge(spec_counts, on='practice_id', how='left')

relation_spec_practice_level['final_speciality'] = relation_spec_practice_level.apply(lambda row: 'Others' if row['spec_count'] > 1 else row['speciality'],axis=1)

relation_spec_practice_level = relation_spec_practice_level.drop(columns=['spec_count'])
relation_spec_practice_level1=relation_spec_practice_level[['practice_id','final_speciality']].rename(columns={'final_speciality':'speciality'})
relation_spec_practice_level1.drop_duplicates(inplace=True)
relation_spec_practice_level1.dropna(subset='speciality',inplace=True)
raw_data_relation_id_0=raw_data[raw_data['relation_id']==0]
raw_data_relation_id_non0=raw_data[raw_data['relation_id']!=0]
raw_data_relation_id_0.shape
raw_data_relation_id_0=raw_data_relation_id_0.merge(relation_spec_practice_level1, on='practice_id',how='left')
raw_data_relation_id_0.shape
raw_data_relation_id_0.drop(columns='speciality_x', inplace=True)
raw_data_relation_id_0.rename(columns={'speciality_y':'speciality'},inplace=True)
raw_data_final = pd.concat([raw_data_relation_id_0, raw_data_relation_id_non0], ignore_index=True)

#assured and poc bills
poc_bills = clinic_bills_df.copy()
assured_bills = assured_bills_df.copy()

poc_bills['practice_id'] = pd.to_numeric(poc_bills['practice_id'], errors='coerce')
poc_bills = poc_bills.dropna(subset=['practice_id'])

poc_txn_summary=poc_txn_summary.merge(poc_bills, left_on=['practice_id','year_month'],right_on=['practice_id','month'], how="left")
poc_txn_summary['practo_share1']=poc_txn_summary['practo_share']/poc_txn_summary['poc_txn_count']

avg_share = poc_txn_summary['practo_share1'].mean()
poc_txn_summary['practo_share1'].fillna(avg_share, inplace=True)
poc_txn_summary = poc_txn_summary[['practice_id', 'year_month', 'practo_share1']].rename(columns={'practo_share1': 'practo_share'})

# Clean up intermediate variables
# del poc_bills, avg_share
# gc_module.collect()

assured_bills['practice_id']=assured_bills['practice_id'].astype(str)
relation_ids['practice_id']=relation_ids['practice_id'].astype(str)
assured_bills['doctor_id']=assured_bills['doctor_id'].astype(str)
relation_ids['doctor_id']=relation_ids['doctor_id'].astype(float)
relation_ids['doctor_id']=relation_ids['doctor_id'].astype(str)

df_poc_monetised=df_poc_monetised.merge(poc_txn_summary, on=['practice_id', 'year_month'], how='left')
df_poc_monetised.head()
df_poc_monetised1=df_poc_monetised.copy()
assurd_bills=assured_bills.merge(relation_ids, on=['practice_id','doctor_id'],how='left')

assured_txn_summary['practice_id']=assured_txn_summary['practice_id'].astype(str)
assured_txn_summary['relation_id']=assured_txn_summary['relation_id'].astype(float)
assured_txn_summary['relation_id']=assured_txn_summary['relation_id'].astype(str)
assurd_bills['relation_id']=assurd_bills['relation_id'].astype(str)
assurd_bills['practice_id']=assurd_bills['practice_id'].astype(float)
assurd_bills['practice_id']=assurd_bills['practice_id'].astype(str)

df_assured_monetised['practice_id']=df_assured_monetised['practice_id'].astype(str)
df_assured_monetised['relation_id']=df_assured_monetised['relation_id'].astype(float)
df_assured_monetised['relation_id']=df_assured_monetised['relation_id'].astype(str)

assured_txn_summary = (df_assured_monetised.groupby(['practice_id', 'relation_id', 'year_month']).size().reset_index(name='assured_txn_count'))
assured_txn_summary=assured_txn_summary.merge(assurd_bills,left_on=['practice_id','relation_id','year_month'],right_on=['practice_id','relation_id','month'], how='left')
assured_txn_summary['month'] = pd.to_numeric(assured_txn_summary['month'], errors='coerce')
assured_txn_summary['practo_share_y'] = assured_txn_summary['month'] / assured_txn_summary['assured_txn_count']
assured_txn_summary['practo_share'].fillna(avg_share, inplace=True)
avg_share = (assured_txn_summary.groupby(['practice_id', 'relation_id'])['practo_share_y'].transform(lambda x: x.fillna(x.mean())))
assured_txn_summary['practo_share_y'] = assured_txn_summary['practo_share_y'].fillna(avg_share)
avg_practo_share = assured_txn_summary['practo_share_y'].mean()
assured_txn_summary['practo_share_y'] = assured_txn_summary['practo_share_y'].fillna(avg_practo_share).replace("NaN",avg_practo_share)

# Clean up intermediate variables
# del avg_share, avg_practo_share, assurd_bills
# gc_module.collect()

assured_txn_summary=assured_txn_summary[['practice_id','relation_id','year_month','practo_share_y']]
df_assured_monetised.head()
df_assured_monetised.drop(columns='practo_share',inplace=True)

df_assured_monetised = df_assured_monetised.merge(assured_txn_summary,on=['practice_id', 'relation_id', 'year_month'],how='left')
df_assured_monetised = df_assured_monetised.rename(columns={'practo_share_y':'practo_share'})

df_others.columns,df_assured_monetised.columns,df_poc_monetised1.columns
df_assured_monetised = df_assured_monetised.rename(columns={'practo_share_y': 'practo_share'})

common_cols = ['id', 'event_date', 'type', 'practice_id', 'relation_id',
               'is_monetisable', 'city', 'mobile', 'program_name', 'is_monetised',
               'speciality', 'channel', 'platform', 'search_type',
               'revenue', 'month',  'practo_share']

df_others.columns,df_assured_monetised.columns,df_poc_monetised1.columns
df_poc_monetised1.rename(columns={'practo_share_y':'practo_share'},inplace=True)

raw_data_final = pd.concat([df_others[common_cols],df_assured_monetised[common_cols],df_poc_monetised1[common_cols]], ignore_index=True)
raw_data_final = raw_data_final.sort_values(by=['mobile', 'event_date']).reset_index(drop=True)

# Clean up intermediate variables after concatenation
# del df_others, df_assured_monetised, df_poc_monetised1, raw_data_relation_id_0, raw_data_relation_id_non0
# del relation_spec_practice_level1, relation__spec, spec_counts, relation_spec_practice_level
# del doc_spec_query, relation_ids
# gc_module.collect()

avg=raw_data_final.groupby('type')['practo_share'].mean().reset_index()
avg1=raw_data_final.groupby('program_name')['practo_share'].mean().reset_index()
df_retsil=raw_data_final[raw_data_final['type']=='retail_plan_cx'].sort_values(by='month')
df1_filtered=raw_data_final

#Reading the sheet and fetching the data from sheet
gs = pygsheets.authorize(service_file='/Users/banajabhishekchanappa/Downloads/data/service_account.json')
cac_cor = gs.open_by_key('1KLVyX2n3uLp1rK6q4rlv97bCZInzeec-tPnc9fhPULQ')
value_sheet = cac_cor.worksheet('id', 1709595522)
values = value_sheet.get_as_df(start='a1', end='c')

df1_filtered['event_date'] = pd.to_datetime(df1_filtered['event_date'])
df1_filtered['txn_rank'] = df1_filtered.groupby('mobile')['event_date'].rank(method='first')
df1_filtered['tag'] = df1_filtered['txn_rank'].apply(lambda x: 'cac' if x == 1 else 'cor')
df1_filtered['channel'].fillna("NULL", inplace=True)
df1_filtered['speciality'].fillna("Others", inplace=True)
df1_filtered=df1_filtered.merge(values,on=['type','channel'], how='left')
df1_filtered.loc[df1_filtered['type'] == 'retail_plan_cx', 'practo_share'] = 137
df1_filtered['practo_share'] = pd.to_numeric(df1_filtered['practo_share'], errors='coerce')
df1_filtered['value'] = pd.to_numeric(df1_filtered['value'], errors='coerce')
df1_filtered['contribution']=df1_filtered['practo_share']-df1_filtered['value']
final_data=df1_filtered[['id','event_date','type','is_monetisable','city','mobile','program_name','is_monetised','revenue','month','practo_share','contribution']]
final_data['event_date'] = pd.to_datetime(final_data['event_date'])
final_data['month'] = pd.to_datetime(final_data['month'])

# Clean up intermediate variables
# del avg, avg1, df_retsil, df1_filtered, values, cac_cor, value_sheet
# gc_module.collect()

monthly = (final_data.groupby(['mobile','month']).agg(contribution=('contribution','sum'),gross_txns=('id','count'),monetised_txns=('is_monetised','sum')).reset_index().sort_values(['mobile','month']))

# Convert to Polars (if not already)
monthly_pl = pl.from_pandas(monthly)
monthly_pl = monthly_pl.with_columns([
    pl.col('contribution').rolling_sum(window_size=12, min_periods=1).over('mobile').alias('contribution_12m'),
    pl.col('gross_txns').rolling_sum(window_size=12, min_periods=1).over('mobile').alias('gross_txns_12m'),
    pl.col('monetised_txns').rolling_sum(window_size=12, min_periods=1).over('mobile').alias('monetised_txns_12m')])
monthly = monthly_pl.to_pandas()

# Conditions for gross_txns_12m, monetised_txns_12m
monthly_pl = pl.from_pandas(monthly)
monthly_pl = monthly_pl.with_columns(
    pl.when(
        (pl.col('contribution_12m') >= 1000) & 
        (pl.col('gross_txns_12m') >= 4) & 
        (pl.col('monetised_txns_12m') >= 1)
    ).then(pl.lit("PLATINUM"))
    .when(
        (pl.col('contribution_12m') >= 200) & 
        (pl.col('gross_txns_12m') >= 2) & 
        (pl.col('monetised_txns_12m') >= 1)
    ).then(pl.lit("GOLD"))
    .when(
        (pl.col('contribution_12m') >= 0) & 
        (pl.col('monetised_txns_12m') >= 1)
    ).then(pl.lit("SILVER"))
    .when(
        (pl.col('contribution_12m') < 0) & 
        (pl.col('monetised_txns_12m') >= 1)
    ).then(pl.lit("COPPER"))
    .when(pl.col('gross_txns_12m') >= 3).then(pl.lit("BASE HEAVY"))
    .when(pl.col('gross_txns_12m') >= 2).then(pl.lit("BASE MEDIUM"))
    .when(pl.col('gross_txns_12m') >= 1).then(pl.lit("BASE LIGHT"))
    .otherwise(pl.lit("UNTAGGED"))
    .alias("tag")
)
monthly = monthly_pl.to_pandas()
print("✓ Monthly tags computed")

# Clean up Polars DataFrame after conversion
# del monthly_pl
# gc_module.collect()

final_data = final_data.merge(monthly[['mobile','month','tag']],on=['mobile','month'],how='left')
final_data['event_date'] = pd.to_datetime(final_data['event_date'])
final_data['month'] = pd.to_datetime(final_data['month'])

# 🔹 Find each user's latest tag based on latest event_date
latest_tags = (final_data.sort_values('event_date').groupby('mobile', as_index=False).last()[['mobile', 'tag']].rename(columns={'tag': 'latest_tag'}))

# 🔹 Merge back to assign latest tag to all transactions
final_data = final_data.merge(latest_tags, on='mobile', how='left')
final_data['tag'] = final_data['latest_tag']
final_data = final_data.drop(columns='latest_tag')

# Ensure dates are datetime
final_data['event_date'] = pd.to_datetime(final_data['event_date'])
final_data['month'] = pd.to_datetime(final_data['month'])

# Auto-generate target months from July 2025 → current month
start_month = pd.Timestamp("2025-07-01")
end_month = pd.Timestamp(pd.Timestamp.today().strftime("%Y-%m-01"))

target_months = pd.date_range(start=start_month, end=end_month, freq="MS").to_list()

results = []

for target in target_months:
    
    # 1️⃣ Find latest event_date available for that target month
    mask_current_month = final_data['month'] == target

    if not final_data.loc[mask_current_month, 'event_date'].empty:
        latest_date = final_data.loc[mask_current_month, 'event_date'].max()
    else:
        continue  

    # 2️⃣ Rolling 12-month window based on latest event_date
    start = latest_date - pd.DateOffset(months=12) + pd.Timedelta(days=1)
    end = latest_date

    # 3️⃣ Filter window data
    hist = final_data[(final_data['event_date'] >= start) &(final_data['event_date'] <= end)]

    # 4️⃣ Aggregation - Per mobile + tag
    agg = (hist.groupby(['mobile', 'tag']).agg(txns=('id', 'count'),monetised_txns=('is_monetised', 'sum'),gm=('practo_share', 'sum')).reset_index())
    agg['month'] = target

    # 5️⃣ New vs Retained logic
    if target == target_months[0]:
        agg['status'] = "New"
    else:
        prev_targets = [m for m in target_months if m < target]
        prev_hist = final_data[
            (final_data['month'].isin(prev_targets)) &
            (final_data['event_date'] > (target - pd.DateOffset(months=12))) &
            (final_data['event_date'] <= target)
        ][['mobile', 'tag']].drop_duplicates()
        prev_pairs = set(zip(prev_hist['mobile'], prev_hist['tag']))
        agg['status'] = ["Retained" if (m, t) in prev_pairs else "New" for m, t in zip(agg['mobile'], agg['tag'])]
        
    summary = (
        agg.groupby(['month', 'tag', 'status'])
        .agg(
            count=('mobile', 'nunique'),
            txns=('txns', 'sum'),
            monetised_txns=('monetised_txns', 'sum'),
            gm=('gm', 'sum')
        )
        .reset_index()
    )

    results.append(summary)
    
final_summary = pd.concat(results, ignore_index=True)
print("✓ Final summary computed ")

final_summary['gm']=final_summary['gm'].astype(int)
current_month = final_summary['month'].max()
current_data = final_summary[final_summary['month'] == current_month].copy()
current_data.loc[current_data['monetised_txns'] == 0, 'gm'] = 0

# Reorder columns: month should come first
current_data = current_data[['month', 'tag', 'status', 'count', 'txns', 'monetised_txns', 'gm']]

# Push to Google Sheets
sheet = gs.open_by_key('1WU14C1SJVhO_575jqZuw67gK6IlP5ghW-B9ZtA8vjMk')
worksheet = sheet.worksheet_by_title('PGSCB_data')

# Clear existing data and update
worksheet.clear()
worksheet.set_dataframe(current_data, start='A1')

print("✓ Data pushed to dashboard")
