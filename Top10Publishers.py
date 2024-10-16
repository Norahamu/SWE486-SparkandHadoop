
import os
import pandas as pd
import chardet
import plotly.express as px

# CSV directory
csv_directory = '/workspaces/Spark-jupyter/data/Arabic/*.csv'
all_data = pd.DataFrame()
failed_files = []

# Function to detect encoding
def detect_encoding(file_path):
    """Detect the encoding of a file."""
    with open(file_path, 'rb') as f:
        result = chardet.detect(f.read(10000))  
    return result['encoding']

# Load data from CSV files
for filename in os.listdir(csv_directory):
    if filename.endswith('.csv'):
        file_path = os.path.join(csv_directory, filename)
        encoding = detect_encoding(file_path) 
        try:
            temp_data = pd.read_csv(file_path, encoding=encoding, on_bad_lines='skip')  # Skip bad lines
            all_data = pd.concat([all_data, temp_data], ignore_index=True)
            print(f"Loaded {filename} with encoding {encoding}")
        except Exception as e:
            print(f"Failed to load {filename} with detected encoding {encoding}: {e}")
            failed_files.append(filename) 

            try:
                temp_data = pd.read_csv(file_path, encoding='ISO-8859-1', on_bad_lines='skip')
                all_data = pd.concat([all_data, temp_data], ignore_index=True)
                print(f"Successfully loaded {filename} with fallback encoding ISO-8859-1")
            except Exception as e:
                print(f"Failed to load {filename} with fallback encoding: {e}")
                failed_files.append(filename)

print("Data loaded:", all_data.shape)

# Function to plot top publishers
def plot_top_publishers(data):
    if not data.empty and 'sitename' in data.columns and 'crawl_date' in data.columns:
        data['crawl_date'] = pd.to_datetime(data['crawl_date'], errors='coerce')
        data['year'] = data['crawl_date'].dt.year

        total_publications = data.groupby('sitename').size().reset_index(name='publication_count')
        total_publications = total_publications.sort_values(by='publication_count', ascending=False).head(10)

        fig = px.bar(total_publications, x='publication_count', y='sitename', orientation='h',
                     title='Top 10 Publishers by Total Publications',
                     labels={'sitename': 'Publisher', 'publication_count': 'Total Publications'})

        fig.update_layout(yaxis_title='Publisher', xaxis_title='Total Publications')
        fig.show()

        print("Top 10 Publishers:")
        print(total_publications)
    else:
        print("The DataFrame is empty or does not contain the necessary columns.")

# Check for required columns
required_columns = ['sitename', 'crawl_date']
missing_columns = [col for col in required_columns if col not in all_data.columns]

if missing_columns:
    print("Missing columns:", missing_columns)
else:
    print("All required columns are present.")
    plot_top_publishers(all_data)
