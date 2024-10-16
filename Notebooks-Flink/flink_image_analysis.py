from pyflink.table import EnvironmentSettings, TableEnvironment
import pandas as pd
import os
import chardet
import plotly.express as px

# Set up the Flink Table Environment
env_settings = EnvironmentSettings.new_instance().in_batch_mode().build()
table_env = TableEnvironment.create(env_settings)

# Define the path to the CSV directory
csv_directory = '/workspaces/Spark-jupyter/data/Arabic/*.csv'

# Initialize a list to track failed files
failed_files = []

# Function to detect encoding
def detect_encoding(file_path):
    """Detect the encoding of a file."""
    with open(file_path, 'rb') as f:
        result = chardet.detect(f.read(10000))  
    return result['encoding']

# Initialize an empty DataFrame to store loaded data
all_data = pd.DataFrame()

# Load CSV files
for filename in os.listdir(csv_directory):
    if filename.endswith('.csv'):
        file_path = os.path.join(csv_directory, filename)
        encoding = detect_encoding(file_path)
        
        try:
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

# Process data: Check for image presence
all_data['image_present'] = all_data['image_url'].notnull() & (all_data['image_url'].str.strip() != '')

# Filter the data for present images
filtered_data = all_data[all_data['image_present']]

# Group by categories and count images
image_counts = filtered_data.groupby('categories').size().reset_index(name='image_count')

# Get the top N categories
top_n = 10
image_counts = image_counts.nlargest(top_n, 'image_count')

# Plotting using Plotly
fig = px.bar(image_counts, x='image_count', y='categories', orientation='h',
             title='Number of Images per Article Category',
             labels={'categories': 'Categories', 'image_count': 'Number of Images'},
             color='image_count', 
             color_continuous_scale=px.colors.sequential.Viridis)  # Change to a valid color scale

fig.update_layout(yaxis_title='Categories', xaxis_title='Number of Images')
fig.show()

print("Analysis complete. Number of images per category has been plotted.")
