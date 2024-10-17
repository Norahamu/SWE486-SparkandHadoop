import os
import pandas as pd
import matplotlib.pyplot as plt
import chardet
from pyflink.table import EnvironmentSettings, TableEnvironment

# Set up the Flink Table Environment
env_settings = EnvironmentSettings.new_instance().in_batch_mode().build()
table_env = TableEnvironment.create(env_settings)

# Define the path to the CSV directory
csv_directory = '/workspaces/Spark-jupyter/Dataset/*.csv'
all_data = pd.DataFrame()
failed_files = []

def detect_encoding(file_path):
    """Detect the encoding of a file."""
    with open(file_path, 'rb') as f:
        result = chardet.detect(f.read(10000))  
    return result['encoding']

# Load CSV files
for filename in os.listdir(csv_directory):
    if filename.endswith('.csv'):
        file_path = os.path.join(csv_directory, filename)
        encoding = detect_encoding(file_path) 

        try:
            # Load CSV file into a Pandas DataFrame first
            temp_data = pd.read_csv(file_path, encoding=encoding, on_bad_lines='skip')  
            all_data = pd.concat([all_data, temp_data], ignore_index=True)
            print(f"Loaded {filename} with encoding {encoding}")
        except Exception as e:
            print(f"Failed to load {filename} with detected encoding {encoding}: {e}")
            failed_files.append(filename)

            try:
                # Attempt to load with a fallback encoding
                temp_data = pd.read_csv(file_path, encoding='ISO-8859-1', on_bad_lines='skip')
                all_data = pd.concat([all_data, temp_data], ignore_index=True)
                print(f"Successfully loaded {filename} with fallback encoding ISO-8859-1")
            except Exception as e:
                print(f"Failed to load {filename} with fallback encoding: {e}")
                failed_files.append(filename)

# Output the shape of the loaded data
print("Data loaded:", all_data.shape)

if not all_data.empty and 'crawl_date' in all_data.columns and 'sitename' in all_data.columns:
    all_data['crawl_date'] = pd.to_datetime(all_data['crawl_date'], errors='coerce')
    all_data['year'] = all_data['crawl_date'].dt.year  

    publications_per_year = all_data.groupby(['year', 'sitename']).size().reset_index(name='publication_count')
    publications_per_year = publications_per_year[publications_per_year['year'] >= 2013]

    max_publications_year = publications_per_year.loc[publications_per_year['publication_count'].idxmax()]

    plt.figure(figsize=(12, 8))
    plt.bar(publications_per_year['year'], publications_per_year['publication_count'], color='skyblue', edgecolor='blue')
    
    plt.grid(axis='y', linestyle='--', alpha=0.7)

    plt.xlabel('Year', fontsize=14)
    plt.ylabel('Number of Publications', fontsize=14)
    plt.title('Number of Publications per Year', fontsize=16)

    plt.xticks(publications_per_year['year'], rotation=45, ha='right')

    plt.axvline(x=max_publications_year['year'], color='red', linestyle='--', label=f"Most Publications in {int(max_publications_year['year'])}")

    plt.legend()

    plt.tight_layout()
    plt.show()

    print(f"The year with the most publications is {int(max_publications_year['year'])} with {int(max_publications_year['publication_count'])} publications.")
else:
    print("The DataFrame is empty or does not contain the necessary columns.")
