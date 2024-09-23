import pandas as pd

def analyze_temperature_data(file_path):
    # Read the CSV file
    df = pd.read_csv(file_path, parse_dates=['timestamp'], sep=';', decimal=',')

    # Ensure the timestamp column is in datetime format
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # Set the timestamp as the index
    df.set_index('timestamp', inplace=True)

    # Resample the data to daily frequency and calculate statistics
    daily_stats = df['temperature'].resample('D').agg(['max', 'mean', 'min', 'std'])

    return daily_stats

def main():
    file_path = 'data/Tem 7_14-8-12.csv'  # Replace with your actual file path
    daily_stats = analyze_temperature_data(file_path)
    print(daily_stats)

if __name__ == "__main__":
    main()