# language: python
import pandas as pd

# Read the CSV file; adjust the file path if needed.
df = pd.read_csv("SP500.csv")

# Define the columns to transform.
columns_to_convert = ['open', 'high', 'low', 'close']

# Multiply each specified column by 100 and convert to integer.
for col in columns_to_convert:
    df[col] = (df[col] * 100).astype(int)

# Write the transformed DataFrame to a new CSV file.
df.to_csv("SP500_converted.csv", index=False)
print("Converted CSV file has been generated as 'SP500_converted.csv'.")
