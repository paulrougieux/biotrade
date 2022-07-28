# Importing the CronTab class from the module
from crontab import CronTab

## Using the current user
cron = CronTab(user=True)
# Creating a new job
job = cron.new(command="test_cron.py")
job.minute.every(1)
cron.write()

