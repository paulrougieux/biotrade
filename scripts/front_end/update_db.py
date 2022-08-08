"""
Script which updates biotrade database
"""

from biotrade.faostat import faostat

faostat.pump.update(["country"])
# faostat.pump.update(["crop_production", "crop_trade"])
# faostat.pump.update(["forestry_production", "forestry_trade", "forest_land"])
# faostat.pump.update(["food_balance"])
# faostat.pump.update(["land_use", "land_cover"])
