from  bigpato import bigpato
import pandas as pd
import streamlit as st

bq_project = 'velascoluis-dev-sandbox'
bq_database = 'tpcds_100G'
#Under this dataset, we have all the TPCDS tables
bq_key = 'secrets/secret-key.json'
duckdb_db = 'my-db-tpcds.duckdb'
local_duck_folder = 'datasets/bigpato'
export_bq_bucket = 'bigpato-export-bucket'

if 'bigpato_client' not in st.session_state:
    bp_client = bigpato.BigPato(bq_project=bq_project, bq_database=bq_database, bq_key=bq_key,
                                     duckdb_db=duckdb_db, local_duck_folder=local_duck_folder, export_bq_bucket=export_bq_bucket)
    st.session_state['bigpato_client'] = bp_client


bigpato_client = st.session_state['bigpato_client']

query_1 = "SELECT * from web_page"
query_2 = "SELECT * from warehouse"
#Query72 TPC-DS
query_3 = "SELECT i_item_desc, w_warehouse_name, d1.d_week_seq, SUM(CASE  WHEN p_promo_sk IS NULL THEN 1 ELSE 0  END) no_promo, SUM(CASE  WHEN p_promo_sk IS NOT NULL THEN 1 ELSE 0 END) promo, count(*) total_cnt  FROM   catalog_sales  JOIN inventory  ON ( cs_item_sk = inv_item_sk )  JOIN warehouse  ON ( w_warehouse_sk = inv_warehouse_sk )  JOIN item  ON ( i_item_sk = cs_item_sk )  JOIN customer_demographics  ON ( cs_bill_cdemo_sk = cd_demo_sk )  JOIN household_demographics  ON ( cs_bill_hdemo_sk = hd_demo_sk )  JOIN date_dim d1  ON ( cs_sold_date_sk = d1.d_date_sk )  JOIN date_dim d2  ON ( inv_date_sk = d2.d_date_sk )  JOIN date_dim d3  ON ( cs_ship_date_sk = d3.d_date_sk )  LEFT OUTER JOIN promotion  ON ( cs_promo_sk = p_promo_sk )  LEFT OUTER JOIN catalog_returns  ON ( cr_item_sk = cs_item_sk  AND cr_order_number = cs_order_number )  WHERE  d1.d_week_seq = d2.d_week_seq  AND inv_quantity_on_hand < cs_quantity  AND d3.d_date > d1.d_date + INTERVAL '5' day  AND hd_buy_potential = '501-1000'  AND d1.d_year = 2002  AND cd_marital_status = 'M'  GROUP  BY i_item_desc,  w_warehouse_name,  d1.d_week_seq  ORDER  BY total_cnt DESC,  i_item_desc,  w_warehouse_name,  d1.d_week_seq  LIMIT  100"


st.title("Big🦆 demo")
st.text("This demo shows how to integrate BigPato smart query client in a streamlit application")
st.text("Query 1 : {}".format(query_1))

if st.button('Exec Q1'):
    st.text("Executing query 1 ...")
    df = bigpato_client.exec_query(query_1)
    st.text("OK")
    st.dataframe(df)
st.text("Query 2 : {}".format(query_2))
if st.button('Exec Q2'):
    st.text("Executing query 2 ...")
    df = bigpato_client.exec_query(query_2)
    st.text("OK")
    st.dataframe(df)
st.text("Query 3 : {}".format(query_3))
if st.button('Exec Q3'):
    st.text("Executing query 3 ...")
    df = bigpato_client.exec_query(query_3)
    st.text("OK")
    st.dataframe(df)

custom_query = st.text_input('Enter custom query', 'select foo from bar')
if st.button('Exec custom query'):
    st.text("Executing query 3 ...")
    df = bigpato_client.exec_query(custom_query)
    st.text("OK")
    st.dataframe(df)


if st.button('View BigPato metadata dict'):
    st.text("Accesing metadata ...")
    st.text(bigpato_client.get_metadata_dict())
    st.text("OK")


if st.button('View BigPato LRU cache'):
    st.text("Accesing cache ...")
    st.text(bigpato_client.get_cache())
    st.text("OK")

if st.button('Rebalance storage'):
    st.text("Rebalancing storage ...")
    bigpato_client.launch_balance_storage()
    st.text("Rebalancing finished!")



