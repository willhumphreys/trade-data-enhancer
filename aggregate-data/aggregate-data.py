import matplotlib.pyplot as plt
import pandas as pd


def load_and_process_data(file_path):
    # Read the CSV file
    df = pd.read_csv(file_path)

    # Print first few rows to see what we're working with
    print("Sample of original data:")
    print(df.head(2))

    # Convert the Date column to datetime
    df['DateTime'] = pd.to_datetime(df['Date'])

    # Convert DateTime to epoch seconds for output
    df['Timestamp'] = df['DateTime'].apply(lambda x: x.timestamp())

    # Print the date range to verify our conversion
    print(f"Date range in the data: {df['DateTime'].min()} to {df['DateTime'].max()}")

    # We'll handle only the columns we need
    required_columns = ['Timestamp', 'DateTime', 'Open', 'High', 'Low', 'Close']

    # Determine which volume column to use (either 'Volume BTC' or 'Volume')
    if 'Volume BTC' in df.columns:
        volume_col = 'Volume BTC'
    elif 'Volume' in df.columns:
        volume_col = 'Volume'
    else:
        raise ValueError("Could not find volume column in data")

    # Select only the required columns
    if volume_col not in required_columns:
        required_columns.append(volume_col)

    df = df[required_columns]

    # Rename the volume column to 'Volume' if it's not already
    if volume_col != 'Volume':
        df = df.rename(columns={volume_col: 'Volume'})

    # Set the datetime as the index for resampling
    df = df.set_index('DateTime')

    # Sort by datetime (earliest first)
    df = df.sort_index()

    return df


def aggregate_to_hourly(df):
    # Resample to hourly data
    hourly_data = df.resample('h').agg({
        'Timestamp': 'first',  # Keep the first timestamp in seconds
        'Open': 'first',
        'High': 'max',
        'Low': 'min',
        'Close': 'last',
        'Volume': 'sum'
    }).dropna()  # Remove empty periods

    return hourly_data


def aggregate_to_daily(df):
    # Resample to daily data
    daily_data = df.resample('D').agg({
        'Timestamp': 'first',  # Keep the first timestamp in seconds
        'Open': 'first',
        'High': 'max',
        'Low': 'min',
        'Close': 'last',
        'Volume': 'sum'
    }).dropna()  # Remove empty periods

    return daily_data


def plot_data(minute_df, hourly_df, daily_df):
    # Create a figure with 3 subplots
    fig, axes = plt.subplots(3, 1, figsize=(12, 16))

    # Plot closing prices for each timeframe
    minute_df['Close'].plot(ax=axes[0], title='Minute Close Prices')
    hourly_df['Close'].plot(ax=axes[1], title='Hourly Close Prices')
    daily_df['Close'].plot(ax=axes[2], title='Daily Close Prices')

    # Adjust layout
    plt.tight_layout()
    plt.savefig('bitcoin_price_comparison.png')
    plt.close()


def save_to_csv(df, filename):
    # Create a copy of the DataFrame to avoid modifying the original
    output_df = df.copy()

    # Reset the index to move DateTime to a column
    output_df = output_df.reset_index()

    # Select and order columns according to desired output format
    output_df = output_df[['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume']]

    # Save to CSV with comma separator
    output_df.to_csv(filename, sep=',', index=False,
                     float_format='%.2f')  # Format floating point numbers

    print(f"Saved: {filename}")

    # Print a sample of the saved data
    print(f"Sample of saved data in {filename}:")
    print(output_df.head(3).to_string(index=False))


def main(file_path):
    # Load and process the data
    minute_data = load_and_process_data(file_path)

    # Print sample of processed data
    print("\nSample of processed minute data (first 3 records):")
    print(minute_data.head(3))

    # Save the processed minute data in the desired format
    save_to_csv(minute_data, 'output/btc-1m.csv')

    # Aggregate to hourly and daily
    hourly_data = aggregate_to_hourly(minute_data)
    daily_data = aggregate_to_daily(minute_data)

    # Save the aggregated data to CSV files
    save_to_csv(hourly_data, 'output/btc-1h.csv')
    save_to_csv(daily_data, 'output/btc-1d.csv')

    # Plot the data
    try:
        plot_data(minute_data, hourly_data, daily_data)
        print("Data visualization completed successfully!")
    except Exception as e:
        print(f"Error during plotting: {e}")

    # Print summary
    print(f"\nData processed successfully!")
    print(f"Minute data: {len(minute_data)} records")
    print(f"Hourly data: {len(hourly_data)} records")
    print(f"Daily data: {len(daily_data)} records")

    # Return the dataframes for further analysis if needed
    return minute_data, hourly_data, daily_data


if __name__ == "__main__":
    file_path = "input/btc_data.csv"  # Update this to your file path
    try:
        minute_data, hourly_data, daily_data = main(file_path)
    except Exception as e:
        print(f"An error occurred in the main function: {e}")

        # Additional diagnostic information
        print("\nAttempting to diagnose the issue:")
        try:
            # Try just loading the file and inspecting the structure
            df = pd.read_csv(file_path)
            print(f"Columns in file: {df.columns.tolist()}")
            print(f"Date column format example: {df['Date'].iloc[0]}")
            print(f"Sample record: {df.iloc[0].to_dict()}")
        except Exception as diag_err:
            print(f"Diagnostic failed: {diag_err}")
