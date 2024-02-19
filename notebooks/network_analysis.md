---
jupyter:
  jupytext:
    formats: ipynb,md
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.3'
      jupytext_version: 1.14.0
  kernelspec:
    display_name: Python 3 (ipykernel)
    language: python
    name: python3
---

Network analysis of soya trade to detect change in exporter fluxes

```python
# Import dependencies
import pandas as pd
from biotrade.comtrade import comtrade
from biotrade.faostat import faostat
import networkx as nx
import plotly.graph_objs as go
```

```python
# Select products
product_codes = ["120100", "120110", "120190"]
# Select countries
country_codes = faostat.country_groups.df["iso3_code"].dropna().tolist()
# Select South America as partners
# partner_codes = list(
#     df_country[
#         (df_country["continent"] == "Americas")
#         & (df_country["sub_continent"] == "South America")
#     ]["un_code"]
# )
# # Select MS, China and USA as reporters
# reporter_codes = [*list(df_country[df_country["eu27"] == 1]["un_code"]), 156, 842]
trade_data = comtrade.db.select(
    table="yearly",
    product_code=product_codes,
    flow="import",
    # reporter_code=reporter_codes,
    # partner_code=partner_codes,
    period_start=2018,
    period_end=2023,
)
trade_data = trade_data[
    (trade_data[["reporter_iso", "partner_iso"]].isin(country_codes)).all(axis=1)
].reset_index(drop=True)
cols = [
    "reporter_iso",
    "partner_iso",
    "year",
]
# Aggregate over products
trade_data = trade_data.groupby(cols).agg({"net_weight": "sum"}).reset_index()
# Mean of export fluxes for each country
avg_exp = (
    (
        trade_data.groupby(["partner_iso", "year"]).agg({"net_weight": "sum"})
        / trade_data.groupby(["year"]).agg({"net_weight": "sum"})
        * 100
    )
    .reset_index()
    .rename(columns={"partner_iso": "country_iso", "net_weight": "value"})
)
avg_exp["type"] = "os-c"
# Mean of imports fluxes for each country
avg_imp = (
    (
        trade_data.groupby(["reporter_iso", "year"]).agg({"net_weight": "sum"})
        / trade_data.groupby(["year"]).agg({"net_weight": "sum"})
        * 100
    )
    .reset_index()
    .rename(columns={"reporter_iso": "country_iso", "net_weight": "value"})
)
avg_imp["type"] = "is-c"
# Concat results
avg = pd.concat([avg_imp, avg_exp], ignore_index = True)
```

```python
df = pd.DataFrame()
for year in sorted(trade_data["year"].unique()):
    G = nx.DiGraph()
    df_year = trade_data[trade_data["year"] == year]
    records = df_year.apply(
        lambda row: (
            row["partner_iso"],
            row["reporter_iso"],
            {"weight": row["net_weight"]},
        ),
        axis=1,
    ).tolist()
    G.add_edges_from(records)
    btc = nx.betweenness_centrality(G)
    btc = (
        pd.DataFrame.from_dict(data=btc, orient="index", columns=["value"])
        .reset_index()
        .rename(columns={"index": "country_iso"})
    )
    btc["year"] = year
    btc["type"] = "bt-c"
    df = pd.concat([df, btc], ignore_index=True)
df = pd.concat([df, avg], ignore_index=True)
```

```python
# Plot
fig = go.Figure()

# Per ogni riga nel DataFrame
for index, row in df.sort_values(by="iso3_code").iterrows():
    # Aggiungi una traccia per ogni nazione
    fig.add_trace(
        go.Scatter(
            x=df.columns[1:],
            y=row[1:],
            mode="lines",
            name=row["iso3_code"],
            visible="legendonly",
        )
    )

# Imposta il layout del grafico
fig.update_layout(
    title="Soyabeans",
    xaxis_title="Time [y]",
    yaxis_title="Bewtweeness centrality",
    hovermode="closest",
)

# Visualizza il grafico
fig.show()
```
