import pandas as pd
import streamlit as st
from deltalake import DeltaTable

from lakehouse.paths import GOLD_DIR


@st.cache_data
def table(name):
    return DeltaTable(str(GOLD_DIR / name)).to_pandas()


st.set_page_config(page_title="CFPB complaints", layout="wide")
st.title("CFPB consumer complaints")

monthly, companies, states = table("complaints_by_month"), table("company_scorecard"), table("state_year")
monthly["month"] = pd.to_datetime(monthly["month"])

total = int(monthly["complaints"].sum())
timely = (companies["timely_rate"].fillna(0) * companies["complaints"]).sum() / companies["complaints"].sum()
a, b, c = st.columns(3)
a.metric("Complaints", f"{total:,}")
b.metric("Companies", f"{len(companies):,}")
c.metric("Answered on time", f"{timely:.1%}")

top = monthly.groupby("product")["complaints"].sum().nlargest(5).index.tolist()
picked = st.sidebar.multiselect("Products", sorted(monthly["product"].unique()), default=top)
min_n = st.sidebar.number_input("Minimum complaints per company", 1, 1_000_000, 1000)

st.subheader("Complaints per month")
st.line_chart(monthly[monthly["product"].isin(picked)].pivot_table(index="month", columns="product", values="complaints", aggfunc="sum"))

left, right = st.columns(2)
with left:
    st.subheader("Companies that give relief most often")
    big = companies[companies["complaints"] >= min_n].sort_values("relief_rate", ascending=False)
    st.dataframe(big.head(20), hide_index=True)
with right:
    year = st.selectbox("Year", sorted(states["year"].unique(), reverse=True))
    st.subheader(f"Complaints by state, {year}")
    st.bar_chart(states[states["year"] == year].set_index("state")["complaints"].sort_values(ascending=False).head(25))
