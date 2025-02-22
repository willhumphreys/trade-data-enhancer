# language: python
import numpy as np
import pandas as pd

# Define the ATR period for daily data.
PERIOD = 14

# Read the CSV file with headers.
# Update the file path as necessary.
df = pd.read_csv("/home/will/IdeaProjects/TickDataEnhancer/data/output/5_hourly_atr_es-1hF.csv", header=0)

# Parse the 'Timestamp' column into a datetime column.
df['dateTime'] = pd.to_datetime(df['Timestamp'], unit='s')

# Ensure the numeric columns are correctly converted.
for col in ['open', 'high', 'low', 'close', 'volume']:
    df[col] = pd.to_numeric(df[col], errors='coerce')

# Sort the DataFrame by the datetime column if not already sorted.
df.sort_values(by='dateTime', inplace=True)

# Aggregate data into daily bars.
daily_df = df.resample('D', on='dateTime').agg({
    'open': 'first',  # Daily open is the first open of the day.
    'high': 'max',  # Daily high is the maximum high of the day.
    'low': 'min',  # Daily low is the minimum low of the day.
    'close': 'last',  # Daily close is the last close of the day.
    'volume': 'sum'  # Total volume for the day.
}).reset_index()

# Calculate True Range for daily bars.
daily_df['prev_close'] = daily_df['close'].shift(1)
daily_df['range'] = daily_df['high'] - daily_df['low']
daily_df['tr_diff_high'] = (daily_df['high'] - daily_df['prev_close']).abs()
daily_df['tr_diff_low'] = (daily_df['low'] - daily_df['prev_close']).abs()

# The True Range is the maximum of the three components.
daily_df['true_range'] = daily_df[['range', 'tr_diff_high', 'tr_diff_low']].max(axis=1)
daily_df['true_range'] = daily_df['true_range'].fillna(daily_df['range'])

# Initialize a list for the calculated ATR values.
atr_values = [np.nan] * len(daily_df)

if len(daily_df) >= PERIOD:
    # Compute the initial ATR as the simple average of the first PERIOD true ranges.
    initial_atr = daily_df['true_range'].iloc[:PERIOD].mean()
    atr_values[PERIOD - 1] = initial_atr

    # Apply Wilder's smoothing method for subsequent rows.
    for i in range(PERIOD, len(daily_df)):
        previous_atr = atr_values[i - 1]
        current_tr = daily_df.at[i, 'true_range']
        atr_values[i] = (previous_atr * (PERIOD - 1) + current_tr) / PERIOD

daily_df['atr_14'] = atr_values

# Optionally remove intermediate calculation columns.
daily_df.drop(columns=['prev_close', 'range', 'tr_diff_high', 'tr_diff_low', 'true_range'], inplace=True)

# Write the resulting daily ATR values to a new CSV file.
daily_df.to_csv("daily_atr14_from_hourly.csv", index=False)
print(
    "New CSV file with the daily ATR (aggregated from hourly data) has been created as 'daily_atr14_from_hourly.csv'.")
