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
import os
from pathlib import Path
from plotly.subplots import make_subplots
import plotly.express as px

output_folder = Path(os.environ["BIOTRADE_DATA"]) / "plots" / "centrality"
```

```python
# Select products
product_codes = ["230400"]
# Select countries
df_country = faostat.country_groups.df
country_codes = df_country["iso3_code"].dropna().tolist()
# Select only certain souther countries
country_codes = ["ARG", "BRA", "PRY", "URY", "BOL"]
# Select all reporters and partners
reporter_codes = None
partner_codes = None
# Select only South America trades
reporter_codes = list(
    df_country[
        (df_country["continent"] == "Americas")
        & (df_country["sub_continent"] == "South America")
    ]["un_code"]
)
partner_codes = reporter_codes
trade_data = comtrade.db.select(
    table="yearly",
    product_code=product_codes,
    flow="import",
    reporter_code=reporter_codes,
    partner_code=partner_codes,
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
    )
    .reset_index()
    .rename(columns={"reporter_iso": "country_iso", "net_weight": "value"})
)
avg_imp["type"] = "is-c"
# Concat results
avg = pd.concat([avg_imp, avg_exp], ignore_index=True)
```

```python
df = pd.DataFrame()
# Calculate betweeness centrality for each year
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
# Concat results
df = pd.concat([df, avg], ignore_index=True)
# Put as zeros values < 0.01
df.loc[df["value"] < 0.001, "value"] = 0
# Pivot table
df = df.pivot(values="value", index=["country_iso", "year"], columns="type")
```

```python
product = "soya_cake"
region = "south_america"
subplot = True
if subplot:
    # Create a subplot with bar charts for each country
    fig = make_subplots(
        rows=1, cols=len(df.index.levels[0]), subplot_titles=sorted(df.index.levels[0])
    )
    fig.update_layout(
        barmode="group",
        title_text=f"{product.replace('_', ' ').title()}",
        xaxis_title="Time [y]",
        yaxis_title="Centrality",
        height=1000,
        width=2000,
        font=dict(size=24),
    )
    for i, country in enumerate(sorted(df.index.levels[0])):
        country_df = df.loc[country]
        colors = px.colors.qualitative.Plotly[:3]
        color_dict = {
            "bt-c": colors[0],
            "is-c": colors[1],
            "os-c": colors[2],
        }
        if i == 0:
            legend = True
        else:
            legend = False
        for column in color_dict.keys():
            fig.add_trace(
                go.Bar(
                    x=country_df.index.get_level_values("year"),
                    y=country_df[column],
                    name=column,
                    marker_color=color_dict[column],
                    showlegend=legend,
                ),
                row=1,
                col=i + 1,
            )
    fig.update_annotations(font_size=24)
    fig.update_yaxes(range=[0, 1])
    fig.write_image(output_folder / f"{product}_{region}_trade.png")
    df.to_csv(output_folder / f"{product}_{region}_trade.csv")
else:
    for country in sorted(df.index.levels[0]):
        # Create a bar chart for each country
        fig = go.Figure()
        country_df = df.loc[country]
        for col in ["bt-c", "is-c", "os-c"]:
            fig.add_trace(
                go.Bar(
                    x=country_df.index.get_level_values("year"),
                    y=country_df[col],
                    name=col,
                )
            )
            fig.update_layout(
                barmode="group",
                title_text=f"{product.replace('_', ' ').title()}",
                xaxis_title="Time [y]",
                yaxis_title="Centrality",
            )
        fig.write_image(output_folder / f"{product}_{region}_trade_{country}.png")
```
