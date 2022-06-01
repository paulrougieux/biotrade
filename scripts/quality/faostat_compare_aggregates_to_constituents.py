#!/usr/bin/env python
# coding: utf-8

""" Check that aggregates of forestry products correspond to the sum of their constituents

The purpose of this notebook is to compare aggregate products to the sum of
their constituents. Comparisons are done with the `biotrade.faostat.quality`
function `check_prod_agg_and_parts`.

Run this script at the command line with:

    ipython -i ~/repos/forobs/biotrade/scripts/quality/faostat_compare_aggregates_to_constituents.py

See also the notebook:

    ~/repos/forobs/obs3df_documents/notebooks/by_product/faostat_compare_aggregates_to_constituents.md

"""

# Internal modules
from biotrade.faostat.quality import check_prod_agg_and_parts
from biotrade.faostat import faostat


# Load FAOSTAT production data
fp = faostat.db.select("forestry_production")

#######################################################################################
# Compare production and trade of aggregate products to the sum of their constituents #
#######################################################################################

# ## Roundwood and sawnwood
# Check that roundwood production is the sum of industrial roundwood and fuel wood
# Also check for export and import values
check_prod_agg_and_parts(fp, "roundwood", ["industrial_roundwood", "wood_fuel"])

check_prod_agg_and_parts(
    fp, "sawnwood", ["sawnwood_coniferous", "sawnwood_non_coniferous_all"]
)

# ## Wood based panels
panel_constituents = [
    "plywood",
    "particle_board_and_osb_1961_1994_",
    "particle_board",
    "osb",
    "hardboard",
    "mdf_hdf",
    "other_fibreboard",
    "fibreboard_compressed_1961_1994_",
]
check_prod_agg_and_parts(
    fp.query("year> 1997"), "wood_based_panels", panel_constituents
)
# This will fail because some constituents only existed for recent years
# check_prod_agg_and_parts(
#     fp, "wood_based_panels", panel_constituents
# )

# ## Paper and paper board
paper_constituents = [
    "newsprint",
    "printing_and_writing_papers_uncoated_mechanical",
    "printing_and_writing_papers_uncoated_wood_free",
    "printing_and_writing_papers_coated",
    "household_and_sanitary_papers",
    "wrapping_and_packaging_paper_and_paperboard_1961_1997_",
    "case_materials",
    "cartonboard",
    "wrapping_papers",
    "other_papers_mainly_for_packaging",
    "other_paper_and_paperboard_n_e_s_not_elsewhere_specified_",
]
# Level 1 aggregate
# Constituents only exist for recent years
check_prod_agg_and_parts(
    fp.query("year > 1997"), "paper_and_paperboard", paper_constituents
)
# Constituents were not reported for old years, this will fail
check_prod_agg_and_parts(fp, "paper_and_paperboard", paper_constituents)

# Paper constituents level 2
paper_constituents_level_2 = [
    "graphic_papers",
    "other_paper_and_paperboard",
]
check_prod_agg_and_parts(fp, "paper_and_paperboard", paper_constituents_level_2)

# Printing and writing paper constituents
pwp_constituents = [
    "printing_and_writing_papers_uncoated_mechanical",
    "printing_and_writing_papers_uncoated_wood_free",
    "printing_and_writing_papers_coated",
]
# Constituents only exist for recent years
check_prod_agg_and_parts(
    fp.query("year > 1997"), "printing_and_writing_papers", pwp_constituents
)

# Constituents were not reported for old years, therefore this will fail
# check_prod_agg_and_parts(
#     fp, 'printing_and_writing_papers', pwp_constituents
# )

# Pulp
# Check that roundwood production is the sum of industrial roundwood and fuel wood
# Also check for export and import values
# Other aggregates
#    "pulp_for_paper",
#    "wood_pulp",
#     "wood_pulp_excluding_mechanical_wood_pulp",
#     "chemical_wood_pulp",
# Products not in wood pulp
#     "pulp_from_fibres_other_than_wood",
#     "recovered_fibre_pulp",
pulp_constituents = [
    "mechanical_and_semi_chemical_wood_pulp",
    "mechanical_wood_pulp",
    "semi_chemical_wood_pulp",
    "chemical_wood_pulp_sulphate_unbleached",
    "chemical_wood_pulp_sulphate_bleached",
    "chemical_wood_pulp_sulphite",
    "chemical_wood_pulp_sulphite_unbleached",
    "chemical_wood_pulp_sulphite_bleached",
    "dissolving_wood_pulp",
]
check_prod_agg_and_parts(fp, "wood_pulp", pulp_constituents)
