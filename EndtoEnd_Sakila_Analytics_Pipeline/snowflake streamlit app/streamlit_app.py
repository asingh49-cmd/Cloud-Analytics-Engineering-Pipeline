
# Streamlit in Snowflake app for Sakila DW
# Save as: app.py
# In Snowsight: Works with Streamlit in Snowflake (no external installs needed)

import streamlit as st
import pandas as pd
import numpy as np
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, lit, to_date, when
from datetime import date

# Setup
session = get_active_session()

DB = "SAKILA_DW"
SCHEMA = "PUBLIC"

FACT = f"{DB}.{SCHEMA}.FACT_RENTAL"
DIM_CUSTOMER = f"{DB}.{SCHEMA}.DIM_CUSTOMER"
DIM_FILM = f"{DB}.{SCHEMA}.DIM_FILM"
DIM_STORE = f"{DB}.{SCHEMA}.DIM_STORE"
DIM_DATE = f"{DB}.{SCHEMA}.DIM_DATE"

st.set_page_config(page_title="Sakila Analytics (Streamlit in Snowflake)", layout="wide")

# Header with account info (for screenshots)
acct = session.sql("select current_account() as account, current_region() as region, current_user() as user, current_role() as role, current_warehouse() as wh").to_pandas().iloc[0]
st.markdown(f"**Account:** `{acct['ACCOUNT']}`  •  **Region:** `{acct['REGION']}`  •  **User:** `{acct['USER']}`  •  **Role:** `{acct['ROLE']}`  •  **Warehouse:** `{acct['WH']}`")
st.title("Sakila Analytics — Streamlit in Snowflake")

# Stores
stores_df = session.sql(f"""
    SELECT DISTINCT store_id
    FROM {DIM_STORE}
    WHERE store_id IS NOT NULL
    ORDER BY store_id
""").to_pandas()
store_choices = [int(x) for x in stores_df["STORE_ID"].tolist()] if not stores_df.empty else []

col_f1, col_f2, col_f3 = st.columns(3)

with col_f1:
    selected_stores = st.multiselect("Filter: Store ID(s)", store_choices, default=store_choices[:2] if store_choices else [])

# Dates (using DIM_DATE."date" and FACT_RENTAL.rental_date_key)
minmax = session.sql(f"""
    SELECT MIN("date") AS MIN_DATE, MAX("date") AS MAX_DATE
    FROM {DIM_DATE}
    WHERE "date" IS NOT NULL
""").to_pandas()

default_min = pd.to_datetime(minmax["MIN_DATE"].iloc[0]).date() if minmax.notnull().all(axis=None) else date(2006,1,1)
default_max = pd.to_datetime(minmax["MAX_DATE"].iloc[0]).date() if minmax.notnull().all(axis=None) else date(2006,12,31)

with col_f2:
    date_range = st.date_input("Filter: Date range", value=(default_min, default_max))

with col_f3:
    show_sections = st.multiselect(
        "Sections to display",
        [
            "Top 10 customers by revenue",
            "Monthly revenue trend by category",
            "Store performance",
            "Inventory utilization",
            "Avg rental duration by category",
        ],
        default=[
            "Top 10 customers by revenue",
            "Monthly revenue trend by category",
            "Store performance",
        ],
    )

date_start, date_end = date_range if isinstance(date_range, (list, tuple)) else (default_min, default_max)


# Common filtered base
# Map rental_date_key -> DIM_DATE."date"
base_df = session.sql(f"""
    SELECT
      f.RENTAL_ID,
      f.RENTAL_LAST_UPDATE,
      f.CUSTOMER_KEY,
      f.STAFF_KEY,
      f.FILM_KEY,
      f.STORE_KEY,
      f.RENTAL_DATE_KEY,
      f.RETURN_DATE_KEY,
      f.COUNT_RENTALS,
      f.COUNT_RETURNS,
      f.RENTAL_DURATION,
      f.DOLLAR_AMOUNT AS PAYMENT_AMOUNT,
      d."date" AS RENTAL_DATE
    FROM {FACT} f
    LEFT JOIN {DIM_DATE} d
      ON f.RENTAL_DATE_KEY = d.DATE_KEY
    WHERE d."date" BETWEEN TO_DATE('{date_start}') AND TO_DATE('{date_end}')
""")

if selected_stores:
    base_df = base_df.filter(col("store_key").isin([lit(int(s)) for s in selected_stores]))

base_pdf = base_df.to_pandas()

# Convenience lookups
cust_lkp = session.sql(f"""
    SELECT customer_key, customer_id, customer_first_name, customer_last_name
    FROM {DIM_CUSTOMER}
""").to_pandas()

store_lkp = session.sql(f"""
    SELECT store_key, store_id, store_manager_first_name, store_manager_last_name
    FROM {DIM_STORE}
""").to_pandas()

film_pdf = session.sql(f"""
    SELECT *
    FROM {DIM_FILM}
""").to_pandas()

# Derive category names dynamically from DIM_FILM columns (film_in_category_*)
category_cols = [c for c in film_pdf.columns if c.startswith("FILM_IN_CATEGORY_")]
def melt_categories(df_film: pd.DataFrame) -> pd.DataFrame:
    m = df_film[["FILM_KEY"] + category_cols].copy()
    m = m.melt(id_vars="FILM_KEY", var_name="CATEGORY_FLAG", value_name="IN_CAT")
    m = m[m["IN_CAT"].astype(str).isin(["1", "True", "TRUE", "true"])]
    m["FILM_CATEGORY"] = m["CATEGORY_FLAG"].str.replace("FILM_IN_CATEGORY_", "", regex=False).str.title()
    return m[["FILM_KEY", "FILM_CATEGORY"]].drop_duplicates()

film_cat_map = melt_categories(film_pdf) if not film_pdf.empty and category_cols else pd.DataFrame(columns=["FILM_KEY","FILM_CATEGORY"])


# Section: Top 10 customers by rental revenue
if "Top 10 customers by revenue" in show_sections:
    st.subheader("Top 10 Customers by Rental Revenue")
    if base_pdf.empty:
        st.info("No data for selected filters.")
    else:
        # Group by CUSTOMER_KEY (not CUSTOMER_ID)
        top = (
            base_pdf.groupby("CUSTOMER_KEY", dropna=True)["PAYMENT_AMOUNT"]
            .sum()
            .reset_index()
            .rename(columns={"PAYMENT_AMOUNT": "TOTAL_REVENUE"})
            .sort_values("TOTAL_REVENUE", ascending=False)
            .head(10)
        )

        # attach names from DIM_CUSTOMER (customer_key)
        if not cust_lkp.empty:
            top = top.merge(
                cust_lkp[["CUSTOMER_KEY", "CUSTOMER_FIRST_NAME", "CUSTOMER_LAST_NAME"]],
                on="CUSTOMER_KEY",
                how="left",
            )
            top["CUSTOMER_NAME"] = (
                top[["CUSTOMER_FIRST_NAME", "CUSTOMER_LAST_NAME"]]
                .fillna("")
                .agg(" ".join, axis=1)
                .str.strip()
            )
        else:
            top["CUSTOMER_NAME"] = top["CUSTOMER_KEY"].astype(str)

        st.bar_chart(top.set_index("CUSTOMER_NAME")["TOTAL_REVENUE"])
        st.dataframe(top[["CUSTOMER_KEY", "CUSTOMER_NAME", "TOTAL_REVENUE"]])



# Section: Monthly revenue trend by film category
if "Monthly revenue trend by category" in show_sections:
    st.subheader("Monthly Revenue Trend by Film Category")
    if base_pdf.empty or film_cat_map.empty:
        st.info("No data or categories unavailable for selected filters.")
    else:
        # Join films (by FILM_KEY) -> category membership
        joined = base_pdf.merge(film_pdf[["FILM_KEY"]], left_on="FILM_KEY", right_on="FILM_KEY", how="left")
        joined = joined.merge(film_cat_map, left_on="FILM_KEY", right_on="FILM_KEY", how="left")
        # Month
        joined["RENTAL_MONTH"] = pd.to_datetime(joined["RENTAL_DATE"]).dt.to_period("M").dt.to_timestamp()
        cat_month = (
            joined.dropna(subset=["RENTAL_MONTH", "PAYMENT_AMOUNT", "FILM_CATEGORY"])
            .groupby(["RENTAL_MONTH","FILM_CATEGORY"])["PAYMENT_AMOUNT"]
            .sum()
            .reset_index()
            .rename(columns={"PAYMENT_AMOUNT":"REVENUE"})
        )
        if cat_month.empty:
            st.info("No category revenue found for the current filters.")
        else:
            # Pivot for multi-series line chart
            pivot = cat_month.pivot(index="RENTAL_MONTH", columns="FILM_CATEGORY", values="REVENUE").fillna(0)
            st.line_chart(pivot)
            st.dataframe(cat_month.sort_values(["RENTAL_MONTH","REVENUE"], ascending=[True, False]))

# Section: Store performance

if "Store performance" in show_sections:
    st.subheader("Store Performance (Revenue, Rentals, Avg Ticket)")
    if base_pdf.empty:
        st.info("No data for selected filters.")
    else:
        perf = (
            base_pdf.groupby("STORE_KEY", dropna=True)
            .agg(REVENUE=("PAYMENT_AMOUNT","sum"),
                 RENTALS=("COUNT_RENTALS","sum"),
                 TXNS=("RENTAL_ID","count"))
            .reset_index()
        )
        perf["AVG_TICKET"] = perf["REVENUE"] / perf["TXNS"].replace(0, np.nan)

        # attach store id / manager
        if not store_lkp.empty:
            perf = perf.merge(store_lkp, on="STORE_KEY", how="left")
            perf["STORE_LABEL"] = perf.apply(
                lambda r: f"Store {int(r['STORE_ID'])} ({(r['STORE_MANAGER_FIRST_NAME'] or '')} {(r['STORE_MANAGER_LAST_NAME'] or '')})".strip(),
                axis=1
            )
        else:
            perf["STORE_LABEL"] = perf["STORE_KEY"].astype(str)

        # charts
        c1, c2, c3 = st.columns(3)
        with c1:
            st.metric("Total Revenue", f"${perf['REVENUE'].sum():,.2f}")
        with c2:
            st.metric("Total Rentals", f"{int(perf['RENTALS'].sum())}")
        with c3:
            st.metric("Avg Ticket (overall)", f"${perf['AVG_TICKET'].mean():.2f}")

        st.bar_chart(perf.set_index("STORE_LABEL")[["REVENUE","RENTALS"]])
        st.dataframe(perf[["STORE_LABEL","REVENUE","RENTALS","AVG_TICKET"]].sort_values("REVENUE", ascending=False))


# Section: Inventory utilization (rentals per item)
if "Inventory utilization" in show_sections:
    st.subheader("Inventory Utilization (Rentals per Inventory Item)")
    if base_pdf.empty:
        st.info("No data for selected filters.")
    else:
        inv = (
            base_pdf.groupby("INVENTORY_ID", dropna=True)["COUNT_RENTALS"]
            .sum()
            .reset_index()
            .rename(columns={"COUNT_RENTALS":"RENTALS"})
        )
        # Basic stats
        st.write(f"Items counted: **{len(inv)}**")
        st.write(f"Avg rentals per item: **{inv['RENTALS'].mean():.2f}**")
        # Distribution
        st.bar_chart(inv["RENTALS"].value_counts().sort_index())

        st.dataframe(inv.sort_values("RENTALS", ascending=False).head(25))

# Section: Avg rental duration by category

if "Avg rental duration by category" in show_sections:
    st.subheader("Average Rental Duration by Film Category")
    if base_pdf.empty or film_cat_map.empty:
        st.info("No data or categories unavailable for selected filters.")
    else:
        j2 = base_pdf.merge(film_cat_map, on="FILM_KEY", how="left")
        dur = (
            j2.dropna(subset=["FILM_CATEGORY", "RENTAL_DURATION"])
            .groupby("FILM_CATEGORY")["RENTAL_DURATION"]
            .mean()
            .reset_index()
            .rename(columns={"RENTAL_DURATION":"AVG_RENTAL_DURATION"})
        )
        if dur.empty:
            st.info("No duration data available under current filters.")
        else:
            st.bar_chart(dur.set_index("FILM_CATEGORY")["AVG_RENTAL_DURATION"])
            st.dataframe(dur.sort_values("AVG_RENTAL_DURATION", ascending=False))

st.caption("Built with Streamlit in Snowflake • Sakila Demo")
