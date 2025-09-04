import os
import pandas as pd
import psycopg2
import streamlit as st
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

from dotenv import load_dotenv

load_dotenv()

DB = {
    "host": os.getenv("PG_HOST", "127.0.0.1"),
    "port": int(os.getenv("PG_PORT", "5433")),
    "dbname": os.getenv("PG_DB", "retail"),
    "user": os.getenv("PG_USER", "admin"),
    "password": os.getenv("PG_PASSWORD", "admin"),
}

@st.cache_data(ttl=300)
def query(sql: str) -> pd.DataFrame:
    with psycopg2.connect(**DB) as conn:
        return pd.read_sql(sql, conn)
    
st.set_page_config(page_title= "Retail Data Platform", layout="wide")
st.title("Retail Sales Overview")

#KPI
kpi = query("""
SELECT
    SUM(amount)::numeric(12,2) AS revenue,
    AVG(amount)::numeric(12,2) AS aov,
    COUNT(*)::bigint AS orders
FROM public_marts.fact_sales;
""").iloc[0]

c1, c2, c3 = st.columns(3)
c1.metric("Revenue", f"${kpi['revenue']}")
c2.metric("Average Order Value", f"${kpi['aov']}")
c3.metric("Orders", f"{kpi['orders']}")

# Daily revenue trend
daily = query("""
SELECT order_date::date AS day, SUM(amount) AS revenue
FROM public_marts.fact_sales
GROUP BY day
ORDER BY day;
""")

st.subheader("Daily Revenue")
fig1, ax1 = plt.subplots(figsize=(6, 3))
ax1.plot(daily["day"], daily["revenue"], marker="o", linewidth=1.5)
ax1.set_xlabel("Date")
ax1.set_ylabel("Revenue")
ax1.xaxis.set_major_locator(mdates.AutoDateLocator(maxticks=8))
ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
fig1.autofmt_xdate()
st.pyplot(fig1, clear_figure=True)

# Revenue by category
cats = query("""
SELECT category, SUM(amount) as revenue
FROM public_marts.fact_sales
GROUP BY category
ORDER BY revenue DESC;
""")
st.subheader("Revenue by Category")
fig2, ax2 = plt.subplots()
ax2.bar(cats["category"], cats["revenue"])
ax2.set_xlabel("Category")
ax2.set_ylabel("Revenue")
plt.xticks(rotation=30, ha="right")
st.pyplot(fig2)

# Top orders table
orders = query("""
SELECT transaction_id AS order_id,
       customer_id,
       product_id,
       order_date,
       amount
FROM public_marts.fact_sales
ORDER BY amount DESC
LIMIT 50;
""")
st.subheader("Top 50 Orders")
st.dataframe(orders, use_container_width=True, hide_index=True)