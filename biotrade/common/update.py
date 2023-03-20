from biotrade.comtrade import comtrade
from biotrade.faostat import faostat
from biotrade.world_bank import world_bank


def cron_quarterly():
    """Update of yearly datasets every three months"""
    comtrade.pump.update_db(table_name="yearly", frequency="A")
    comtrade.pump.update_db_parameter()
    faostat_tables = [
        "country",
        "crop_production",
        "crop_trade",
        "food_balance",
        "forestry_production",
        "forestry_trade",
        "forest_land",
        "land_use",
        "land_cover",
    ]
    faostat.pump.update(faostat_tables, skip_confirmation=True)
    world_bank.pump.update(
        ["indicator", "indicator_name"], skip_confirmation=True
    )


def cron_bi_weekly():
    """Update of monthly datasets every two weeks"""
    comtrade.pump.update_db(table_name="monthly", frequency="M")


def cron_front_end():
    """Update of web platform datasets every three months"""
    from scripts.front_end import product_list
    from scripts.front_end import annual_variation_harvested_area_production
    from scripts.front_end import average_harvested_area_production
    from scripts.front_end import trends_harvested_area_production
    from scripts.front_end import annual_variation_trade_quantity_value
    from scripts.front_end import average_trade_quantity

    product_list.main()
    annual_variation_harvested_area_production.main()
    average_harvested_area_production.main()
    trends_harvested_area_production.main()
    annual_variation_trade_quantity_value.main()
    average_trade_quantity.main()
