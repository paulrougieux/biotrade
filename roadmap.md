
# Aim

The aim of the package is to analyse the environnemental impact of importing bio based 
commodities.

Status:

- planned
- implemented (date and source file)


## Downloading data 

- Downloading data from FAOSTAT. 

    - Status: implemented in faostat/pump.py (December 2021) for the following datasets.

        datasets = {
            "forestry_production": "Forestry_E_All_Data_(Normalized).zip",
            "forestry_trade": "Forestry_Trade_Flows_E_All_Data_(Normalized).zip",
            "forest_land": "Emissions_Land_Use_Forests_E_All_Data_(Normalized).zip",
            "crop_production": "Production_Crops_Livestock_E_All_Data_(Normalized).zip",
            "crop_trade": "Trade_DetailedTradeMatrix_E_All_Data_(Normalized).zip",
            "land_cover": "Environment_LandCover_E_All_Data_(Normalized).zip",
            "land_use": "Inputs_LandUse_E_All_Data_(Normalized).zip",
        }

- Downloading monthly data from the Comtrade bulk API. 

    - Status: Implemented in comtrade/pump.py (January 2022)

- Downloading yearly data at the 6 digit level from the Comtrade API later. Comtrade 
  does some additional validation on the yearly data so it's useful to have it as a 
  complement to the FAOSTAT data. Status: planned.


## Database structure


