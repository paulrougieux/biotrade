from biotrade.comtrade import comtrade
from biotrade.faostat import faostat
from biotrade.world_bank import world_bank


def cron_quarterly():
    """Update of yearly datasets every three months"""
    comtrade.pump.update_db(table_name="yearly", frequency="A", start_year=2000)
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
    world_bank.pump.update(["indicator", "indicator_name"], skip_confirmation=True)


def cron_bi_weekly():
    """Update of monthly datasets every two weeks"""
    comtrade.pump.update_db(table_name="monthly", frequency="M")


def cron_front_end_production_trade():
    """Update of web platform datasets of production and trade every six months"""
    from scripts.front_end import product_list
    from scripts.front_end import annual_variation_production
    from scripts.front_end import average_production
    from scripts.front_end import trends_production
    from scripts.front_end import annual_variation_trade
    from scripts.front_end import average_trade
    from scripts.front_end import statement

    product_list.main()
    annual_variation_production.main()
    average_production.main()
    trends_production.main()
    annual_variation_trade.main()
    average_trade.main()
    statement.main()


def cron_front_end_lafo():
    """Update of web platform datasets of land footprint every six months"""
    from scripts.front_end import statement_lafo
    from scripts.front_end import product_list_lafo
    from scripts.front_end import annual_variation_lafo
    from scripts.front_end import average_lafo

    statement_lafo.main()
    product_list_lafo.main()
    annual_variation_lafo.main()
    average_lafo.main()
