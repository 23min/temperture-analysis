import os
import pandas as pd

def read_temperature_data(file_path):
    df = pd.read_csv(file_path, parse_dates=['timestamp'], sep=';', decimal=',')
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df.set_index('timestamp', inplace=True)

    return df

def main():
    files = [f for f in os.listdir('data') if f.endswith('.csv')]
    sorted_files = sorted(files)
    
    all_data_list = []
    for file in sorted_files:
        file = 'data/' + file
        print(f"Analyzing {file}")
        data = read_temperature_data(file)
        all_data_list.append(data)

    # Concatenate all the data into a single DataFrame
    all_data = pd.concat(all_data_list)

    # Deduplicate the data by dropping rows with duplicate timestamps
    all_data = all_data[~all_data.index.duplicated(keep='first')]

    # Now, resample the data to daily frequency and calculate statistics
    daily_stats_all = all_data['temperature'].resample('D').agg(['max', 'mean', 'min', 'std'])

    # Drop rows where all statistics are NaN (i.e., no data was available for that day)
    daily_stats_all = daily_stats_all.dropna(how='all')

    # Add columns to indicate if the max temperature is above 26, 27, 28, 29, and 30 degrees (Ja/Nej)
    daily_stats_all['över_26'] = daily_stats_all['max'] >= 26
    daily_stats_all['över_27'] = daily_stats_all['max'] >= 27
    daily_stats_all['över_28'] = daily_stats_all['max'] >= 28
    daily_stats_all['över_29'] = daily_stats_all['max'] >= 29
    daily_stats_all['över_30'] = daily_stats_all['max'] >= 30.0

    # Convert True/False to Ja/Nej for each column
    for col in ['över_26', 'över_27', 'över_28', 'över_29', 'över_30']:
        daily_stats_all[col] = daily_stats_all[col].map({True: 'Ja', False: 'Nej'})

    # Round all numerical columns in the daily stats to 2 decimal places
    daily_stats_all[['max', 'mean', 'min', 'std']] = daily_stats_all[['max', 'mean', 'min', 'std']].round(2)

    # Reset index to add the date as a column, and rename it to 'datum'
    daily_stats_all = daily_stats_all.reset_index()
    daily_stats_all.rename(columns={'timestamp': 'datum'}, inplace=True)

    # Print the daily stats
    pd.set_option('display.max_rows', None)
    print(daily_stats_all)

    # Save the daily stats to a file with a Swedish name and correct separators
    daily_stats_all.to_csv('dagliga_statistik.csv', sep=';', decimal=',', index=False, encoding='utf-8-sig')

    # Group by month and count the number of days where the temperature exceeded the thresholds
    monthly_stats = pd.DataFrame({
        'days_26': (daily_stats_all['över_26'] == 'Ja').groupby(daily_stats_all['datum'].dt.month).sum(),
        'days_27': (daily_stats_all['över_27'] == 'Ja').groupby(daily_stats_all['datum'].dt.month).sum(),
        'days_28': (daily_stats_all['över_28'] == 'Ja').groupby(daily_stats_all['datum'].dt.month).sum(),
        'days_29': (daily_stats_all['över_29'] == 'Ja').groupby(daily_stats_all['datum'].dt.month).sum(),
        'days_30': (daily_stats_all['över_30'] == 'Ja').groupby(daily_stats_all['datum'].dt.month).sum()
    })

    # Calculate the total number of days for which we have data, per month
    days_with_data = daily_stats_all['över_26'].groupby(daily_stats_all['datum'].dt.month).count()

    # Calculate the percentage of days where the temperature was above 26 degrees
    percentage_above_26 = (monthly_stats['days_26'] / days_with_data) * 100

    # Combine the statistics into a final table with Swedish translations
    result = pd.DataFrame({
        'månad': ['Juni', 'Juli', 'Augusti'],  # Swedish month names
        'antal_dagar_data': days_with_data.reindex([6, 7, 8], fill_value=0).values,
        'över_26': monthly_stats['days_26'].reindex([6, 7, 8], fill_value=0).values,
        'över_27': monthly_stats['days_27'].reindex([6, 7, 8], fill_value=0).values,
        'över_28': monthly_stats['days_28'].reindex([6, 7, 8], fill_value=0).values,
        'över_29': monthly_stats['days_29'].reindex([6, 7, 8], fill_value=0).values,
        'över_30': monthly_stats['days_30'].reindex([6, 7, 8], fill_value=0).values,
        'procent_över_26': percentage_above_26.reindex([6, 7, 8], fill_value=0).values 
    })

    # Round the values in the result DataFrame to 2 decimal places
    result['procent_över_26'] = result['procent_över_26'].round(2)

    print(f"\n{result}")

    # Save the monthly stats to a file with a Swedish name
    result.to_csv('manadsoversikt.csv', sep=';', decimal=',', index=False, encoding='utf-8-sig')

    # Calculate the overall max, min, and mean for the entire period
    overall_stats = all_data['temperature'].agg(['max', 'min', 'mean']).round(2)

    # Create a DataFrame for the overall summary
    overall_summary = pd.DataFrame({
        'stat': ['Max', 'Min', 'Mean'],  # Swedish equivalents can be 'Max', 'Min', 'Medel'
        'value': [overall_stats['max'], overall_stats['min'], overall_stats['mean']]
    })

    print(f"\nOverall Summary:\n{overall_summary}")

    # Save the overall summary to a CSV
    overall_summary.to_csv('sammanfattning_hela_perioden.csv', sep=';', decimal=',', index=False, encoding='utf-8-sig')

if __name__ == "__main__":
    main()
