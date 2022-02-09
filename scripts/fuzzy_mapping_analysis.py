"""
Written by Selene Patani

Script used to match data from file "TCF_Conversion Factors_fao.xlsx" (coming from
https://www.fao.org/food-agriculture-statistics/en/). Product descriptions
in english, spanish and french have to be match with product code of
FAOSTAT database. Fuzzy mapping is used to conduct this analysis.
"""


import pandas as pd
import re
import numpy as np
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from pathlib import Path

# Renaming compiler
regex_pat = re.compile(r"\W+")
file_name_fao = Path.cwd() / "scripts" / "TCF_Conversion_Factors_fao.xlsx"
# FAO coefficients
df = pd.read_excel(file_name_fao)
# Renaming products description
df["Item_description"] = (
    df["Item_description"].str.replace(regex_pat, "_", regex=True).str.lower()
)
# FAO Spanish trade
# https://fenixservices.fao.org/faostat/static/bulkdownloads/Producci%C3%B3n_Cultivos_ProductosGanaderia_S_Todos_los_Datos_(Normalizado).zip
file_name_spa = (
    Path.cwd()
    / "scripts"
    / "Comercio_MatrizDetalladaComercio_S_Todos_los_Datos_(Normalizado).csv"
)
df_spa = pd.read_csv(file_name_spa, encoding="latin-1")
# Extract code nr and product description in spanish
code_name_spa = df_spa.drop_duplicates(subset=["Código Producto", "Producto"])[
    ["Código Producto", "Producto"]
]
# Renaming products description
code_name_spa["Producto"] = (
    code_name_spa["Producto"].str.replace(regex_pat, "_", regex=True).str.lower()
)
# FAO French trade
# https://fenixservices.fao.org/faostat/static/bulkdownloads/Commerce_MatricesCommerceDetaillees_F_Toutes_les_Données_(Normalisé).zip
file_name_fra = (
    Path.cwd()
    / "scripts"
    / "Commerce_MatricesCommerceDetaillees_F_Toutes_les_Données.csv"
)
df_fra = pd.read_csv(file_name_fra, encoding="latin-1")
# Extract code nr and product description in french
code_name_fra = df_fra.drop_duplicates(subset=["Code Produit", "Produit"])[
    ["Code Produit", "Produit"]
]
# Renaming products description
code_name_fra["Produit"] = (
    code_name_fra["Produit"].str.replace(regex_pat, "_", regex=True).str.lower()
)
# FAO English trade
# https://fenixservices.fao.org/faostat/static/bulkdownloads/Production_Crops_Livestock_E_All_Data_(Normalized).zip
file_name_eng = (
    Path.cwd() / "scripts" / "Trade_DetailedTradeMatrix_E_All_Data_(Normalized).csv"
)
df_eng = pd.read_csv(file_name_eng, encoding="latin-1")
# Extract code nr and product description in english
code_name_eng = df_eng.drop_duplicates(subset=["Item Code", "Item"])[
    ["Item Code", "Item"]
]
# Renaming products description
code_name_eng["Item"] = (
    code_name_eng["Item"].str.replace(regex_pat, "_", regex=True).str.lower()
)
# Merge different translations to have mapping betwen code and product name
spa_eng_code_name = code_name_spa.merge(
    code_name_eng, how="left", left_on="Código Producto", right_on="Item Code"
)
spa_eng_fra_code_name = code_name_fra.merge(
    spa_eng_code_name, how="left", left_on="Code Produit", right_on="Item Code"
)
del spa_eng_fra_code_name["Código Producto"]
del spa_eng_fra_code_name["Code Produit"]
# Empty data frame for storing the matches
# It contains the original word, match, score, english name of product and code
match1 = pd.DataFrame(columns=["original", "match", "score", "product", "product_code"])
# List to track matches already done
match2 = []
# Converting dataframe column Item_description to list of elements for fuzzy matching
myList1 = df.drop_duplicates("Item_description")["Item_description"].tolist()
myList2 = []
# List of possible matches
myList2.extend(spa_eng_fra_code_name["Item"].tolist())
myList2.extend(spa_eng_fra_code_name["Producto"].tolist())
myList2.extend(spa_eng_fra_code_name["Produit"].tolist())
# Iterator over product
n_list = 0
# Iterator over mapping
score_nr = 0
# Threshold to accept match
threshold = 70
# Compare each element i of myList1 the product names of Fao trade tables myList2
while n_list < len(myList1):
    i = myList1[n_list]
    # Extract matches
    fuzzy_result = process.extract(i, myList2, scorer=fuzz.ratio)
    if fuzzy_result and len(fuzzy_result) > score_nr:
        # Store match word and score
        match = fuzzy_result[score_nr][0]
        score = fuzzy_result[score_nr][1]
        if score >= threshold:
            # If match doesn't exist already, store it
            if match not in match2:
                match2.append(match)
                match1.loc[n_list] = [
                    i,
                    match,
                    score,
                    *(
                        (
                            spa_eng_fra_code_name[
                                (
                                    spa_eng_fra_code_name[
                                        ["Item", "Producto", "Produit"]
                                    ]
                                    == match
                                ).any(axis=1)
                            ][["Item", "Item Code"]]
                        )
                        .values[0]
                        .tolist()
                    ),
                ]
                n_list += 1
                score_nr = 0
            else:
                # Compare the score with the match already stored in match1 df
                match_inside = match1[match1["match"] == match]
                # Continue with next found match because score is higher
                if match_inside["score"].values[0] >= score:
                    # Already matched, continues with i item
                    if match_inside.index[0] == n_list:
                        n_list += 1
                        score_nr = 0
                    else:
                        score_nr += 1
                # Put the new match with higher score and repeat from the beginning n_list loop
                else:
                    match1[match1["match"] == match] = np.nan
                    match1.loc[n_list] = [
                        i,
                        match,
                        score,
                        *(
                            (
                                spa_eng_fra_code_name[
                                    (
                                        spa_eng_fra_code_name[
                                            ["Item", "Producto", "Produit"]
                                        ]
                                        == match
                                    ).any(axis=1)
                                ][["Item", "Item Code"]]
                            )
                            .values[0]
                            .tolist()
                        ),
                    ]
                    n_list = 0
                    score_nr = 0
        else:
            match1.loc[n_list] = np.nan
            n_list += 1
            score_nr = 0
    else:
        match1.loc[n_list] = np.nan
        n_list += 1
        score_nr = 0
# Drop nan values
match1 = match1.dropna().reset_index(drop=True)
# Store the matches as a csv file "fuzzy_match.csv"
match1.to_csv(
    Path.cwd() / "scripts" / "fuzzy_match.csv",
    encoding="utf-8",
    index=False,
    na_rep="NA",
)
