import os
import numpy  as np
import pandas as pd
from zipline.utils.cli import maybe_show_progress
from zipline.utils.calendars import get_calendar

def via_fxcm_csv_daily(path, calendar_name='forex', start=None,end=None):

    # TODO: use FXM downloader here

    # get files to bundle
    _, _, file_names = list(os.walk(path))[0]
    file_names = [f for f in file_names if f[-4:] == '.csv']
    symbols = tuple([ file_name.split('_')[0] for file_name in file_names ])

    # there are inconsistent data before this date
    earliest_date = pd.Timestamp('2001-09-17 00:00:00', tz=None)

    # Define custom ingest function
    def ingest(environ,
               asset_db_writer,
               minute_bar_writer,  
               daily_bar_writer,
               adjustment_writer,
               calendar,
               cache,
               show_progress,
               output_dir,
               # pass these as defaults to make them 'nonlocal' in py2
               start=start,
               end=end):

        print('Making bundle with symbols: ', symbols)

        df_metadata = pd.DataFrame(np.empty(len(symbols), dtype=[
            ('start_date', 'datetime64[ns]'),
            ('end_date', 'datetime64[ns]'),
            ('auto_close_date', 'datetime64[ns]'),
            ('symbol', 'object'),
        ]))

        # We need to feed something that is iterable - like a list or a generator -
        # that is a tuple with an integer for sid and a DataFrame for the data to daily_bar_writer
        data=[]
        sid=0

        # preprocess every symbol
        for file_name in file_names:

            symbol = file_name.split('_')[0]
            print( "symbol=",symbol,"file=",os.path.join(path, file_name))

            df=pd.read_csv(
                    os.path.join(path, file_name),
                    index_col='date',
                    parse_dates=True,
                    ).sort_index()

            # change time to midnight. is 21:00 by default
            df.index = df.index + df.index.map(lambda x: pd.Timedelta(hours=24-x.hour))

            candles_before = len(df)

            # convert to a single OHLC number
            df['open'] = (df['bidopen'] + df['askopen']) / 2
            df['high'] = (df['bidhigh'] + df['askhigh']) / 2
            df['low'] = (df['bidlow'] + df['asklow']) / 2
            df['close'] = (df['bidclose'] + df['askclose']) / 2
            df = df[['open', 'high', 'low', 'close']]

            # forward-fill any missing dates
            df = df.resample("1D").mean()
            df.fillna(method="ffill", inplace=True)

            # remove dates not included in current calendar
            calendar = get_calendar(calendar_name)
            df = calendar.filter_dates(df)

            # dummy volume col
            df['volume'] = 0
            df['volume'] = df['volume'].astype(np.int32) 

            candles_after = len(df)

            print('--Preprocessing summary--\ncandles before: {0}\ncandles after: {1}'.format(
                candles_before,
                candles_after))

            data.append((sid,df))

            # the start date is the date of the first trade and
            start_date = df.index[0]

            if start_date < earliest_date:
                start_date = earliest_date

            # the end date is the date of the last trade
            end_date = df.index[-1]

            # The auto_close date is the day after the last trade.
            ac_date = end_date + pd.Timedelta(days=1)

            # Update our meta data
            df_metadata.iloc[sid] = start_date, end_date, ac_date, symbol

            sid += 1

        daily_bar_writer.write(data, show_progress=True)
        print('Daily data has been written')

        df_metadata['exchange'] = "FXCM"

        print('Metadata:')
        print(df_metadata)

        # Not sure why symbol_map is needed
        symbol_map = pd.Series(df_metadata.symbol.index, df_metadata.symbol)

        asset_db_writer.write(equities=df_metadata)
        adjustment_writer.write()
        print('Metadata has been written')

    return ingest

