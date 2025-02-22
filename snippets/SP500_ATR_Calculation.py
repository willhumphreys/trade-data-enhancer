# language: python
import numpy as np
import pandas as pd

# Define the ATR period for daily data (14 days)
PERIOD = 14

# Read the CSV file (assumes 'dateTime' is parsed as a date)
df = pd.read_csv("SP500_converted.csv", parse_dates=["dateTime"])

# Ensure that the necessary numeric columns are converted to numeric types.
numeric_columns = ['open', 'high', 'low', 'close']
df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors='coerce')

# Calculate the previous day's close.
df['prev_close'] = df['close'].shift(1)

# Compute the three components for the True Range:
# 1. The current high minus the current low.
# 2. The absolute difference between the current high and the previous close.
# 3. The absolute difference between the current low and the previous close.
df['range'] = df['high'] - df['low']
df['tr_diff_high'] = (df['high'] - df['prev_close']).abs()
df['tr_diff_low'] = (df['low'] - df['prev_close']).abs()

# The True Range is the maximum of the three components.
df['true_range'] = df[['range', 'tr_diff_high', 'tr_diff_low']].max(axis=1)
df['true_range'] = df['true_range'].fillna(df['range'])

# Initialize an empty list for the ATR values.
atr_values = [np.nan] * len(df)

# Ensure enough data rows exist to calculate the initial ATR.
if len(df) >= PERIOD:
    # Calculate the first ATR value as the simple average of the first PERIOD true ranges.
    initial_atr = df['true_range'].iloc[:PERIOD].mean()
    atr_values[PERIOD - 1] = initial_atr

    # Apply Wilder's smoothing method for subsequent rows.
    for i in range(PERIOD, len(df)):
        previous_atr = atr_values[i - 1]
        current_tr = df.at[i, 'true_range']
        atr_values[i] = (previous_atr * (PERIOD - 1) + current_tr) / PERIOD

# Add the new ATR column to the DataFrame.
df['atr_14'] = atr_values

# Optionally, remove intermediate calculation columns.
df.drop(columns=['prev_close', 'range', 'tr_diff_high', 'tr_diff_low', 'true_range'], inplace=True)

# Write the updated DataFrame to a new CSV file.
df.to_csv("SP500_with_atr14_converted.csv", index=False)
print("New CSV file with the atr_14 column has been created.")
