# Importing the CronTab class from the module
from crontab import CronTab
from dotenv import dotenv_values
# Using patasel user
cron = CronTab(user="patasel")
# Delete previous jobs and environment variables
cron.remove_all()
cron.env.clear()
cron.write()
# Import environment variables and assign them to the crontab
envs = dotenv_values("/storage/SUSBIOM-TRADE/env_var/.env")
for (name, value) in envs.items():
    cron.env[name] = value
# Update faostat data at 03:45 on day-of-month 8 in every 3rd month
job = cron.new(command="""/storage/SUSBIOM-TRADE/conda/susbiom_trade_env/bin/python -c "from biotrade.faostat import faostat;faostat.pump.update(['country', 'crop_production', 'crop_trade', 'food_balance', 'forestry_production', 'forestry_trade', 'forest_land', 'land_use', 'land_cover'],skip_confirmation=True)" >> /eos/jeodpp/data/projects/SUSBIOM-TRADE/biotrade_data/crontab_faostat.log 2>&1""")
job.minute.on(45)
job.hour.on(3)
job.day.on(8)
job.month.every(3)
cron.write()
# Update comtrade yearly data at 04:45 on day-of-month 8 in every 3rd month
job = cron.new(command="""/storage/SUSBIOM-TRADE/conda/susbiom_trade_env/bin/python -c "from biotrade.comtrade import comtrade;comtrade.pump.update_db(table_name = 'yearly', frequency = 'A')" >> /eos/jeodpp/data/projects/SUSBIOM-TRADE/biotrade_data/crontab_comtrade_yearly.log 2>&1""")
job.minute.on(45)
job.hour.on(4)
job.day.on(8)
job.month.every(3)
cron.write()
# Update comtrade monthly data at 04:45 on day-of-month 2 and 16
job = cron.new(command="""/storage/SUSBIOM-TRADE/conda/susbiom_trade_env/bin/python -c "from biotrade.comtrade import comtrade;comtrade.pump.update_db(table_name = 'monthly', frequency = 'M')" >> /eos/jeodpp/data/projects/SUSBIOM-TRADE/biotrade_data/crontab_comtrade_monthly.log 2>&1""")
job.minute.on(45)
job.hour.on(4)
job.day.on(2,16)
cron.write()
