import pandas as pd
import os

folder_path = r'C:\Users\connor\PycharmProjects\trading\data\base\voo_intraday'
all_files = os.listdir(folder_path)

# Filter out non-CSV files
csv_files = [f for f in all_files if f.startswith('voo')]

# Create a list to hold the dataframes
df_list = []

for csv in csv_files:
    file_path = os.path.join(folder_path, csv)
    try:
        # Try reading the file using default UTF-8 encoding
        df = pd.read_csv(file_path)
        df_list.append(df)
    except UnicodeDecodeError:
        try:
            # If UTF-8 fails, try reading the file using UTF-16 encoding with tab separator
            df = pd.read_csv(file_path, sep='\t', encoding='utf-16')
            df_list.append(df)
        except Exception as e:
            print(f"Could not read file {csv} because of error: {e}")
    except Exception as e:
        print(f"Could not read file {csv} because of error: {e}")

# Concatenate all data into one DataFrame
voo_df = pd.concat(df_list, ignore_index=True)

voo_df = voo_df.sort_values(by=['index'], ascending=True)
voo_df.rename(columns={'index': 'date', 'open': 'open', 'high': 'high', 'low': 'low', 'close': 'close', 'volume': 'volume'}, inplace=True)
# Save the final result to a new CSV file
voo_df.to_csv(os.path.join(folder_path, 'full_voo.csv'), index=False)