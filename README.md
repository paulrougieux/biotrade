This package loads trade data on bio based commodities and products from the UN Comtrade 
and from FAOSTAT.


# Installation

Clone the repository and add it to the PYTHONPATH. You can do this by changing the 
environment variables or by adding the following line to your shell configuration file 
such as `.bash_aliases` 

    export PYTHONPATH="$HOME/repos/biotrade/":$PYTHONPATH

# Usage

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
