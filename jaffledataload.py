import os
import pandas as pd
from sqlalchemy import create_engine


user = 'postgres'
password = 'root'
host = 'localhost'
port = '5432'
database = 'jaffle_shop'


csv_folder = '/home/vaishnavi/csv_files'


# engine = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}')
engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}')

def get_table_name(filename):
    if filename.startswith('raw_'):
        return filename[4:].split('.')[0]
    return filename.split('.')[0]

def load_csv_to_mysql(csv_path, table_name, chunk_size=1000):
    print(f"Loading {csv_path} into table '{table_name}'")
    df = pd.read_csv(csv_path)
    total_rows = len(df)
    print(f"Total rows to load: {total_rows}")

    for start in range(0, total_rows, chunk_size):
        end = min(start + chunk_size, total_rows)
        df_chunk = df.iloc[start:end]
       
        if start == 0:
            if_exists_option = 'replace'
        else:
            if_exists_option = 'append'
        df_chunk.to_sql(name=table_name, con=engine, if_exists=if_exists_option, index=False, schema='jaffle_data')
        print(f"Loaded rows {start} to {end} successfully.")

def main():
   
    files = [f for f in os.listdir(csv_folder) if f.endswith('.csv') and f.startswith('raw_')]

    if not files:
        print(f"No CSV files found in {csv_folder} starting with 'raw_'.")
        return

    for file in files:
        csv_path = os.path.join(csv_folder, file)
        table_name = get_table_name(file)
        load_csv_to_mysql(csv_path, table_name)

if __name__ == "__main__":
    main()
