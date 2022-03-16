
The `biotrade` package harmonizes variable names to analyse biological, physical and 
economic flows. It focuses on the agriculture and forestry sectors, from primary 
production to secondary products transformation. It loads bilateral trade and production 
data from the UN Comtrade and from FAOSTAT.

Extraction rates and waste of supply are taken from the FAO, technical conversion 
factors for agricultural commodities available at: 
https://www.fao.org/economic/the-statistics-division-ess/methodology/methodology-systems/technical-conversion-factors-for-agricultural-commodities/ar/


# Installation

## Base installation 

Install from the main branch of a private repo on gitlab using an
[authentication
token](https://docs.gitlab.com/ee/user/project/deploy_tokens/index.html)

    pip install git+https://gitlab+deploy-token-833444:ByW1T2bJZRtYhWuGrauY@gitlab.com/bioeconomy/forobs/biotrade.git@main

To install a previous version specify the git tag, for example v0.0.1

    pip install git+https://gitlab+deploy-token-833444:ByW1T2bJZRtYhWuGrauY@gitlab.com/bioeconomy/forobs/biotrade.git@v0.0.1


## Installation for contributors

If you plan to contribute to the development of the biotrade package, clone the 
repository and tell python where it is located by adding it to your PYTHONPATH. You can 
do this by changing the environment variables or by adding the following line to your 
shell configuration file such as `.bash_aliases`:

    export PYTHONPATH="$HOME/repos/biotrade/":$PYTHONPATH

Specify where you want to store the data by adding the following environment variable:

    export BIOTRADE_DATA="$HOME/repos/biotrade_data/"

Dependencies are listed in the `install_requires` argument of [setup.py](setup.py).


# Usage

The biotrade package is able to download data from FAOSTAT and UN Comtrade and to store 
it inside a database. By default it will use an SQLite database stored locally in the 
folder defined by the environment variable `BIOTRADE_DATA`. Alternatively, a PostGRESQL 
database can be used if a connection string is defined in the environment variable 
`BIOTRADE_DATABASE_URL`, for example by adding the following to your .bash_aliases or 
.bash_rc:

    export BIOTRADE_DATABASE_URL="postgresql://user@localhost/biotrade"

Note that database queries are abstracted with [SQL 
Alchemy](https://www.sqlalchemy.org/) which is what makes it possible to use different 
database engines. SQLite is convenient for data analysis on laptops. PostGreSQL can be 
used on servers.


## FAOSTAT

Faostat provides agriculture and forestry data on their website https://www.fao.org/faostat/en/#data/

The biotrade package has a `faostat.pump` object that loads a selection of datasets. The 
list of downloaded datasets is visible in `faostat.pump.datasets`. Column names and 
product descriptions are reformatted to snake case in a way that is convenient for 
analysis. The data is then stored into an SQLite database (or PostgreSQL):

    >>> from biotrade.faostat import faostat
    >>> faostat.pump.download_all_datasets()
    >>> faostat.pump.update_db()

Once the data has been loaded into the database, you can query it. For example select 
crop production data for 2 countries

    >>> from biotrade.faostat import faostat
    >>> db = faostat.db_sqlite
    >>> cp2 = db.select(table="crop_production",
    >>>                 reporter=["Portugal", "Estonia"])

Select forestry trade flows data reported by all countries, with
Austria as a partner country:

    >>> ft_aut = db.select(table="forestry_trade",
    >>>                    partner=["Austria"])

Select crop trade flows reported by the Netherlands where Brazil was a
partner

    >>> ct_nel_bra = db.select(table="crop_trade",
    >>>                        reporter="Netherlands",
    >>>                        partner="Brazil")

Select the mirror flows reported by Brazil, where the Netherlands was a partner

    >>> ct_bra_bel = db.select(table="crop_trade",
    >>>                        reporter="Brazil",
    >>>                        partner="Netherlands")


## Comtrade

See the documentation of the various methods. As an example  here is how to download 
trade data from the Comtrade API and return a data frame, for debugging purposes:

    >>> from biotrade.comtrade import comtrade
    >>> # Other sawnwood
    >>> swd99 = comtrade.pump.download(cc = "440799")
    >>> # Soy
    >>> soy = comtrade.pump.download(cc = "120190")

Display information on column names used for renaming
and dropping less important columns:

    >>> comtrade.column_names

Get the list of products from the Comtrade API

    >>> hs = comtrade.pump.get_parameter_list("classificationHS.json")

Get the list of reporter and partner countries

    >>> comtrade.pump.get_parameter_list("reporterAreas.json")
    >>> comtrade.pump.get_parameter_list("partnerAreas.json")



# Metadata


## Release dates

FAOSTAT release dates are available at :
https://fenixservices.fao.org/faostat/static/releasecalendar/Default.aspx


# Variable definitions and harmonization

- Variables are defined and compared between the data sources in a notebook called 
  [definitions_and_harmonization](notebooks/definitions_and_harmonization.md)

- Variable names are harmonized between the different sources using a mapping table 
  defined in
  [biotrade/config_data/column_names.csv](https://gitlab.com/bioeconomy/biotrade/-/blob/main/biotrade/config_data/column_names.csv)
  See for example how the `product_code` column is called  `PRODUCT_NC` in Eurostat Comext,
  `commodity_code` or `cmdcode` in UN Comtrade and `item_code` in FAOSTAT.

- `snake_case` is the preferred way of naming files and variables in the code. This 
  follows the R [tidyverse style guide for object 
  names](https://style.tidyverse.org/syntax.html) and the python [PEP 
  8](https://www.python.org/dev/peps/pep-0008/#function-and-variable-names) style guide 
  for function and variable names.


# Licence

This software is dual-licenced under both MIT and EUPL 1.2.
See the [LICENCE.md](LICENCE.md) file.


# Acknowledgements

The authors would like to acknowledge ideas and feedback received from the following 
persons: Lucas Sinclair, Roberto Pilli, Mirco Migliavacca, Giovanni Bausano.



