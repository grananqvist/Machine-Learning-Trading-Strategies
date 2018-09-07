import pytz
import pandas as pd
from datetime import time
from zipline.utils.calendars import TradingCalendar
from zipline.utils.memoize import lazyval


class ForexCalendar(TradingCalendar):

    NYT_4PM = time(20)
    NYT_6PM = time(22)

    @lazyval
    def day(self):
        return pd.tseries.offsets.CustomBusinessDay(
            holidays=self.adhoc_holidays,
            calendar=self.regular_holidays,
            weekmask='Sun Mon Tue Wed Thu Fri'
        )

    @property
    def name(self):
        return "forex"

    @property
    def tz(self):
        return pytz.UTC

    @property
    def open_time(self):
        return time(0, 0)

    @property
    def close_time(self):
        return time(23, 59)

    def _calculate_special_opens(self, start, end):
        return self._special_dates(
            self.special_opens,
            self.special_opens_adhoc(start, end),
            start,
            end,
        )

    def _calculate_special_closes(self, start, end):
        return self._special_dates(
            self.special_closes,
            self.special_closes_adhoc(start, end),
            start,
            end,
        )

    def special_opens_adhoc(self, start, end):
        return [
            (self.NYT_6PM, self._sunday_dates(start, end))
        ]

    def special_closes_adhoc(self, start, end):
        return [
            (self.NYT_4PM, self._friday_dates(start, end))
        ]

    def _friday_dates(self, start, end):
        return pd.date_range(start=start,
                             end=end,
                             freq='W-FRI')

    def _sunday_dates(self, start, end):
        return pd.date_range(start=start,
                             end=end,
                             freq='W-SUN')
