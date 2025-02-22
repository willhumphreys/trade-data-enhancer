import numpy as np
import pandas as pd


def calculate_atr(high, low, close, period):
    """
    Calculate the Average True Range (ATR) over a rolling window.
    """
    tr = np.maximum(
        high - low,
        np.maximum(
            np.abs(high - close.shift(1)),
            np.abs(low - close.shift(1))
        )
    )
    return tr.rolling(window=period).mean()


def determine_period_for_14_days(timeframe):
    """
    Determine the rolling window period for a 14-day ATR based on the timeframe.

    Parameters:
        timeframe (str): The data timeframe ('minute', 'hourly', 'daily').

    Returns:
        int: The appropriate rolling period for a 14-day ATR.
    """
    if timeframe == 'minute':
        return 1440 * 14  # 1440 minutes per day
    elif timeframe == 'hourly':
        return 24 * 14  # 24 hours per day
    elif timeframe == 'daily':
        return 14  # 1 value per day
    else:
        raise ValueError(f"Unsupported timeframe: {timeframe}")


def process_data(file_path, timeframe, output_file):
    """
    Process the data to calculate a new 14-day ATR for a given timeframe (minute, hourly, daily),
    and write the output to a file including the pre-existing and new ATR values.

    Parameters:
        file_path (str): Path to the input CSV file.
        timeframe (str): Data timeframe ('minute', 'hourly', 'daily').
        output_file (str): Path to save the output CSV file.

    Returns:
        pd.DataFrame: DataFrame with both pre-existing and new ATR columns.
    """
    # Read the data
    df = pd.read_csv(file_path)

    # Ensure required columns exist
    required_columns = ['high', 'low', 'close']
    if not all(col in df.columns for col in required_columns):
        raise ValueError(f"CSV must contain the required columns: {required_columns}")

    # Convert the dateTime column to datetime type for resampling based on timeframe
    df['dateTime'] = pd.to_datetime(df['dateTime'])

    # If resampling for hourly or daily data is needed
    if timeframe == 'hourly':
        df = df.resample('H', on='dateTime').agg({
            'high': 'max',
            'low': 'min',
            'close': 'last'
        }).dropna()
    elif timeframe == 'daily':
        df = df.resample('D', on='dateTime').agg({
            'high': 'max',
            'low': 'min',
            'close': 'last'
        }).dropna()

    # Determine the period for a 14-day ATR based on the timeframe
    period = determine_period_for_14_days(timeframe)

    # Calculate the new ATR
    df['new_14_day_atr'] = calculate_atr(df['high'], df['low'], df['close'], period)

    # Write the output to a CSV file including the original and new ATR columns
    df.to_csv(output_file, index_label='index')  # Save the file, add index as a column labeled 'index'

    return df


# Example usage:
file_path = '/home/will/IdeaProjects/TickDataEnhancer/data/output/11_missing_hourly_es-1mF.csv'
# file_path = '/home/will/IdeaProjects/TickDataEnhancer/snippets/SP500.csv'

# # For minute data
minute_output_file = 'output/es_minute_output_with_14_day_atr.csv'
df_minute = process_data(file_path, timeframe='minute', output_file=minute_output_file)
print(f"Minute data 14-day ATR calculation written to {minute_output_file}")
#
# # For hourly data
# hourly_output_file = 'hourly_output_with_14_day_atr.csv'
# df_hourly = process_data(file_path, timeframe='hourly', output_file=hourly_output_file)
# print(f"Hourly data 14-day ATR calculation written to {hourly_output_file}")
#
# # For daily data
# daily_output_file = 'daily_output_with_14_day_atr.csv'
# df_daily = process_data(file_path, timeframe='daily', output_file=daily_output_file)
# print(f"Daily data 14-day ATR calculation written to {daily_output_file}")
