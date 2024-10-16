# Required imports
import os
import pandas as pd
import numpy as np
import chardet
import matplotlib.pyplot as plt
import seaborn as sns
import arabic_reshaper
from bidi.algorithm import get_display
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table import DataTypes
from pyflink.dataset import ExecutionEnvironment

# Initialize Flink execution environment
env = ExecutionEnvironment.get_execution_environment()
settings = EnvironmentSettings.new_instance().in_batch_mode().use_blink_planner().build()
table_env = TableEnvironment.create(settings)

# Directory containing CSV files
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

# Check for required columns
if 'crawl_date' not in all_data.columns or 'categories' not in all_data.columns:
    print("Required columns are missing.")
else:
    all_data['crawl_date'] = pd.to_datetime(all_data['crawl_date'], errors='coerce')
    all_data['year'] = all_data['crawl_date'].dt.year

    category_counts = all_data.groupby(['year', 'categories']).size().reset_index(name='count')
    predictions = []

    # Prepare predictions using linear regression
    for category in category_counts['categories'].unique():
        category_data = category_counts[category_counts['categories'] == category]

        if not category_data.empty:
            X = category_data['year'].values.reshape(-1, 1)
            y = category_data['count'].values

            # Create a linear regression model
            model = LinearRegression()
            model.fit(X, y)

            # Predict future for the next 5 years
            max_year = int(category_data['year'].max())
            future_years = np.array(range(max_year + 1, max_year + 6)).reshape(-1, 1)
            future_predictions = model.predict(future_years)

            predictions.append((category, future_predictions.sum()))

    predictions_df = pd.DataFrame(predictions, columns=['category', 'predicted_count'])
    predictions_df['category'] = predictions_df['category'].str.split(';')
    predictions_exploded = predictions_df.explode('category')

    def reshape_arabic_text(text):
        try:
            return get_display(arabic_reshaper.reshape(text))
        except AssertionError:
            print(f"Error reshaping text: {text}")
            return text 

    predictions_exploded['category'] = predictions_exploded['category'].astype(str)
    predictions_exploded['category'] = predictions_exploded['category'].apply(reshape_arabic_text)

    top_n = 10
    category_counts = predictions_exploded['category'].value_counts().nlargest(top_n).reset_index()
    category_counts.columns = ['category', 'predicted_count']

    plt.rcParams['font.family'] = 'Arial'
    plt.figure(figsize=(12, 8))
    sns.set_style('whitegrid')

    barplot = sns.barplot(x='predicted_count', y='category', data=category_counts, hue='category', palette='viridis', dodge=False, legend=False)
    plt.title('Predicted Top 10 Article Counts by Category for the 5 Upcoming Years', fontsize=16, color='darkblue', weight='bold')
    plt.xlabel('Predicted Article Count', fontsize=14, color='darkblue')
    plt.ylabel('Categories', fontsize=14, color='darkblue')

    for index, value in enumerate(category_counts['predicted_count']):
        barplot.text(value + 0.5, index, f'{value:.0f}', color='black', ha='left', va='center', fontsize=12)

    plt.tight_layout()
    plt.show()
    print("Plotting complete. The predicted article counts per category have been visualized.")
