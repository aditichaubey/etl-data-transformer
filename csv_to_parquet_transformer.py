import pandas as pd
# Read CSV file
csv_file_path = 'C://Users//aditi.chaubey_mavenw//Downloads//input.csv'
df = pd.read_csv(csv_file_path)
# Applying transformations (converted in lower case and handle null values)
df.columns = [col.lower() for col in df.columns]
df.fillna(0, inplace=True)
# handle duplicates 
duplicate_count = df.duplicated().sum()

if duplicate_count > 0:
    print(f"Found {duplicate_count} duplicate rows. Removing them...")
    df.drop_duplicates(inplace=True)
else:
    print("No duplicate rows found.")
# Save the DataFrame to a Parquet file
parquet_file_path = 'C://Users//aditi.chaubey_mavenw//Downloads//output.parquet'
df.to_parquet(parquet_file_path, index=False)
print(f"Data has been transformed and saved to {parquet_file_path}")