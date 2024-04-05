
# %%
from pandas.tseries.holiday import USFederalHolidayCalendar

cal = USFederalHolidayCalendar()
holidays = cal.holidays(start='1990-01-01', end='2050-12-31', return_name=True)


holidays.rename('USFederalHolidays').to_csv(r'c:\Temp\USFederalHolidayCalendar.csv', index_label='HolidayDate')


# %%



