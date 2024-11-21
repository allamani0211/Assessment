import pandas as pd
import sqlite3

def extract_data(file_path, region):
    df = pd.read_csv(file_path)
    df['region'] = region
    return df

def transform_data(region_a_df, region_b_df):
    combined_data = pd.concat([region_a_df, region_b_df], ignore_index=True)
    combined_data['total_sales'] = combined_data['QuantityOrdered'] * combined_data['ItemPrice']
    combined_data['net_sale'] = combined_data['total_sales'] - combined_data['PromotionDiscount']
    combined_data = combined_data.drop_duplicates(subset='OrderId')
    combined_data = combined_data[combined_data['net_sale'] > 0]
    return combined_data

def create_db_connection(db_name):
    return sqlite3.connect(db_name)

def create_sales_table(conn):
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sales_data (
            OrderId INTEGER PRIMARY KEY,
            OrderItemId INTEGER,
            QuantityOrdered INTEGER,
            ItemPrice REAL,
            PromotionDiscount REAL,
            region TEXT,
            total_sales REAL,
            net_sale REAL
        )
    ''')
    conn.commit()

def load_data_to_db(conn, df):
    cursor = conn.cursor()
    for row in df.itertuples(index=False):
        cursor.execute('''
            INSERT OR REPLACE INTO sales_data (OrderId, OrderItemId, QuantityOrdered, ItemPrice, PromotionDiscount, region, total_sales, net_sale)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', row)
    conn.commit()

def count_records(conn):
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM sales_data')
    return cursor.fetchone()[0]

def total_sales_by_region(conn):
    cursor = conn.cursor()
    cursor.execute('SELECT region, SUM(total_sales) FROM sales_data GROUP BY region')
    return cursor.fetchall()

def average_sales_per_transaction(conn):
    cursor = conn.cursor()
    cursor.execute('SELECT AVG(total_sales) FROM sales_data')
    return cursor.fetchone()[0]

def check_duplicates(conn):
    cursor = conn.cursor()
    cursor.execute('SELECT OrderId, COUNT(*) FROM sales_data GROUP BY OrderId HAVING COUNT(*) > 1')
    return cursor.fetchall()

# Example usage
region_a_data = extract_data('region_a_sales.csv', 'A')
region_b_data = extract_data('region_b_sales.csv', 'B')
transformed_data = transform_data(region_a_data, region_b_data)

db_conn = create_db_connection('sales_data.db')
create_sales_table(db_conn)
load_data_to_db(db_conn, transformed_data)

# Validate data
print(f'Total records: {count_records(db_conn)}')
print("Total Sales by Region:")
for region, total_sales in total_sales_by_region(db_conn):
    print(f"Region {region}: {total_sales} INR")
print(f'Average sales per transaction: {average_sales_per_transaction(db_conn)} INR')
duplicates = check_duplicates(db_conn)
if not duplicates:
    print("No duplicate OrderIds found.")
else:
    print("Duplicate OrderIds found:", duplicates)
