
This package loads trade and production data on bio based commodities from the UN
Comtrade and from FAOSTAT. Organizing relevant data to analyse biological, physical and
economic processes is the added value of the `biotrade` package. 


# Installation

Clone the repository and add it to the PYTHONPATH. You can do this by changing the 
environment variables or by adding the following line to your shell configuration file 
such as `.bash_aliases` 

    export PYTHONPATH="$HOME/repos/biotrade/":$PYTHONPATH

# Usage

## Comtrade

See the documentation of the various methods. As an example  here is how to download 
trade data from the API and return a data frame, for debugging purposes:

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

## FAOSTAT

Update all FAOSTAT datasets by downloading bulk files,
then storing them into a SQLite database:

    >>> from biotrade.faostat import faostat
    >>> faostat.pump.download_all_datasets()
    >>> faostat.pump.update_sqlite_db()

Note that database operation are done with SQL Alchemy and that it's also possible to 
use a PostGreSQL database engine. SQLite is convenient for data analysis on personal 
laptops. PostGreSQL can be use for servers.



For example select crop production data for 2 countries

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


# Licence

This software is dual-licenced under both MIT and EUPL 1.2.
See the [LICENCE.md](LICENCE.md) file.

