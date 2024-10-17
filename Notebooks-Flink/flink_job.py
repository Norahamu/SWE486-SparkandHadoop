import pandas as pd
import plotly.express as px
import csv
import glob

def read_data(file_path_pattern):
    try:
        # Use glob to match all CSV files in the directory
        all_files = glob.glob(file_path_pattern)
        # Read all CSV files and concatenate them into a single DataFrame
        df = pd.concat((pd.read_csv(f, on_bad_lines='warn', quoting=csv.QUOTE_NONE) for f in all_files), ignore_index=True)
        print(f"Successfully read {len(all_files)} files.")
        return df
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return pd.DataFrame()  # Return an empty DataFrame on error

def plot_most_frequent_categories(data):
    if not data.empty and 'sitename' in data.columns and 'categories' in data.columns:
        # Group by 'sitename' and 'categories' and count occurrences
        category_frequency = data.groupby(['sitename', 'categories']).size().reset_index(name='count')
        # Sort and get the top 10 most frequent categories
        most_frequent_categories = category_frequency.sort_values(by='count', ascending=False).head(10)

        # Create a horizontal bar plot using Plotly
        fig = px.bar(most_frequent_categories, x='count', y='categories', orientation='h',
                     title='Top 10 Most Frequent Categories Across Published Articles',
                     labels={'categories': 'Categories', 'count': 'Frequency'})

        fig.update_layout(yaxis_title='Categories', xaxis_title='Frequency')
        fig.show()

        print("Top 10 Most Frequent Categories:")
        print(most_frequent_categories)
    else:
        print("The DataFrame is empty or does not contain the necessary columns.")

# Specify the correct path to your CSV files
file_path_pattern = '/workspaces/Spark-jupyter/Dataset/*.csv'  # This matches all CSV files in the Arabic directory

# Read the data
data = read_data(file_path_pattern)

# Plot the most frequent categories
plot_most_frequent_categories(data)
