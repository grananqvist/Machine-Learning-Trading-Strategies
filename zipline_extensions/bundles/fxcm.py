import os
import numpy as np
import pandas as pd
from datetime import datetime
from zipline.utils.cli import maybe_show_progress
from zipline.utils.calendars import get_calendar


def via_fxcm_csv_daily(path, calendar_name='forex', start=None, end=None):

    # TODO: use FXM downloader here. meanwhile, static start and end
    start = pd.Timestamp('2001-09-07')
    end = pd.Timestamp('2018-08-31')

    d1_path = os.path.join(path, 'D1/')
    m1_path = os.path.join(path, 'm1/')

    # get files to bundle
    _, _, file_names = list(os.walk(d1_path))[0]
    file_names = [f for f in file_names if f[-4:] == '.csv']
    # TODO: DEBUG
    symbols = tuple([file_name.split('_')[0] for file_name in [file_names[0]]])

    # there are inconsistent data before this date
    earliest_date = pd.Timestamp('2001-09-17 00:00:00', tz=None)

    # Define custom ingest function
    def ingest(environ,
               asset_db_writer,
               minute_bar_writer,
               daily_bar_writer,
               adjustment_writer,
               calendar,
               start_session,
               end_session,
               cache,
               show_progress,
               output_dir):

        print('Making bundle with symbols: ', symbols)
        print(start_session)
        print(end_session)

        start_session = start_session if start_session is not None else start
        end_session = end_session if end_session is not None else end

        df_metadata = pd.DataFrame(
            np.empty(
                len(symbols),
                dtype=[
                    ('start_date', 'datetime64[ns]'),
                    ('end_date', 'datetime64[ns]'),
                    ('first_traded', 'datetime64[ns]'),
                    ('auto_close_date', 'datetime64[ns]'),
                    ('symbol', 'object'),
                ]))

        # We need to feed something that is iterable - like a list or a generator -
        # that is a tuple with an integer for sid and a DataFrame for the data to daily_bar_writer
        data = []
        data_intraday = []
        sid = 0

        # preprocess every symbol
        for symbol in symbols:

            d1_filename = symbol + '_D1.csv'
            m1_filename = symbol + '_m1.csv'

            print("symbol=", symbol, "file=", d1_filename)

            df = preprocess_csv(
                os.path.join(d1_path, d1_filename),
                calendar_name,
                sample_period='1D')
            df = df[(df.index >= start) & (df.index <= end)]
            data.append((sid, df))

            df = preprocess_csv(
                os.path.join(m1_path, m1_filename),
                calendar_name,
                sample_period='1min')
            df = df[(df.index >= start) & (df.index <= end)]
            data_intraday.append((sid, df))

            # the start date is the date of the first trade and
            start_date = df.index[0]

            if start_date < earliest_date:
                start_date = earliest_date

            # the end date is the date of the last trade
            end_date = df.index[-1]

            # The auto_close date is the day after the last trade.
            ac_date = end_date + pd.Timedelta(days=1)

            # Update our meta data
            df_metadata.iloc[sid] = start_date, end_date, start_date, ac_date, symbol

            sid += 1

        daily_bar_writer.write(data, show_progress=True)
        print('Daily data has been written')

        minute_bar_writer.write(data_intraday, show_progress=True)
        print('Intraday data has been written')

        df_metadata['exchange'] = "FXCM"

        print('Metadata:')
        print(df_metadata)

        # Not sure why symbol_map is needed
        symbol_map = pd.Series(df_metadata.symbol.index, df_metadata.symbol)

        asset_db_writer.write(equities=df_metadata)
        adjustment_writer.write()
        print('Metadata has been written')

    return ingest


def preprocess_csv(path, calendar_name, sample_period='1D'):

    df = pd.read_csv(
        path,
        index_col='date',
        parse_dates=True,
    ).sort_index()

    if sample_period == '1D':
        # change time to midnight. is 21:00 by default
        df.index = df.index + df.index.map(
            lambda x: pd.Timedelta(hours=24 - x.hour))

    candles_before = len(df)

    # convert to a single OHLC number
    df['open'] = (df['bidopen'] + df['askopen']) / 2
    df['high'] = (df['bidhigh'] + df['askhigh']) / 2
    df['low'] = (df['bidlow'] + df['asklow']) / 2
    df['close'] = (df['bidclose'] + df['askclose']) / 2
    df = df[['open', 'high', 'low', 'close']] * 100

    # forward-fill any missing dates
    df = df.resample(sample_period).mean()
    df.fillna(method="ffill", inplace=True)

    # remove dates not included in current calendar
    calendar = get_calendar(calendar_name)
    df = calendar.filter_dates(df, intraday=sample_period != '1D')

    # dummy volume col
    df['volume'] = 0
    df['volume'] = df['volume'].astype(np.int32)

    candles_after = len(df)

    print('--Preprocessing summary--\ncandles before: {0}\ncandles after: {1}'.
          format(candles_before, candles_after))

    return df
