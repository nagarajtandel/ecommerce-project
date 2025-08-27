import pandas as pd
from sqlalchemy import create_engine
import mysql.connector
import os

# List of CSV files and their corresponding table names
csv_files = [
    ('customers.csv', 'customers'),
    ('orders.csv', 'orders'),
    ('order_items.csv', 'order_items'),
    ('products.csv', 'products'),
    ('geolocation.csv', 'geolocation'),
    ('sellers.csv','sellers'),
    ('payments.csv', 'payments')  # Added payments.csv for specific handling
]

# Connect to the MySQL database
conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password='Fresher@15',
    database='ecommerce'
)
cursor = conn.cursor()

engine = create_engine(f'mysql+mysqlconnector://root:Fresher%4015@localhost:3306/ecommerce')
df = pd.read_csv("C:\\Users\\Nagaraj\\Downloads\\archive\\customers.csv")
df.to_sql(name='customers', con=engine, if_exists='replace', index=False)

# Folder containing the CSV files
folder_path = 'C:\\Users\\Nagaraj\\Downloads\\archive'

def get_sql_type(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return 'INT'
    elif pd.api.types.is_float_dtype(dtype):
        return 'FLOAT'
    elif pd.api.types.is_bool_dtype(dtype):
        return 'BOOLEAN'
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return 'DATETIME'
    else:
        return 'TEXT'

# for csv_file, table_name in csv_files:
#     file_path = os.path.join(folder_path, csv_file)
    
#     # Read the CSV file into a pandas DataFrame
#     df = pd.read_csv(file_path)
#     df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
#     # Replace NaN with None to handle SQL NULL
#     df = df.where(pd.notnull(df), None)
    
#     # Debugging: Check for NaN values
#     print(f"Processing {csv_file}")
#     print(f"NaN values before replacement:\n{df.isnull().sum()}\n")

#     # Clean column names
#     df = pd.read_csv("products.csv")
#     df.columns = [col.replace(' ', '_').replace('-', '_').replace('.', '_') for col in df.columns]

#     # Generate the CREATE TABLE statement with appropriate data types
#     columns = ', '.join([f'`{col}` {get_sql_type(df[col].dtype)}' for col in df.columns])
#     create_table_query = f'CREATE TABLE IF NOT EXISTS `{table_name}` ({columns})'
#     cursor.execute(create_table_query)

#     # Insert DataFrame data into the MySQL table
#     for _, row in df.iterrows():
#         # Convert row to tuple and handle NaN/None explicitly
#         values = tuple(None if pd.isna(x) else x for x in row)
#         sql = f"INSERT INTO `{table_name}` ({', '.join(['`' + col + '`' for col in df.columns])}) VALUES ({', '.join(['%s'] * len(row))})"
#         cursor.execute(sql, values)

#     # Commit the transaction for the current CSV file
#     conn.commit()

# Close the connection

for csv_file, table_name in csv_files:
    file_path = os.path.join(folder_path, csv_file)
    
    # Read CSV
    df = pd.read_csv(file_path)
    
    # Clean column names
    df.columns = [col.strip().replace(' ', '_').replace('-', '_').replace('.', '_') for col in df.columns]
    
    # Replace NaN with None for SQL NULL
    df = df.where(pd.notnull(df), None)
    
    # Debugging
    print(f"Processing {csv_file}")
    print(f"NaN values before replacement:\n{df.isnull().sum()}\n")
    
    # Create table dynamically
    # Drop existing table
    cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`")

# Generate CREATE TABLE statement
    columns = ', '.join([f'`{col}` {get_sql_type(df[col].dtype)}' for col in df.columns])
    create_table_query = f'CREATE TABLE `{table_name}` ({columns})'
    cursor.execute(create_table_query)

    
    # Insert rows
    for _, row in df.iterrows():
        values = tuple(None if pd.isna(x) else x for x in row)
        sql = f"INSERT INTO `{table_name}` ({', '.join(['`' + col + '`' for col in df.columns])}) VALUES ({', '.join(['%s'] * len(row))})"
        cursor.execute(sql, values)
    
    conn.commit()

    # 1️⃣ Total Sales per Month
# ----------------------------
    query_sales_month = """
        SELECT 
            DATE_FORMAT(order_purchase_timestamp, '%Y-%m') AS month,
            SUM(payment_value) AS total_sales
        FROM orders o
        JOIN payments p ON o.order_id = p.order_id
        GROUP BY month
        ORDER BY month;
        """

    df_sales_month = pd.read_sql(query_sales_month, engine)
    print("=== Total Sales per Month ===")
    print(df_sales_month)
    print("\n")

        # ----------------------------
        # 2️⃣ Top 10 Products by Revenue
        # ----------------------------
    query_top_products = """
                SELECT 
                pr.product_id,
                pr.product_category,
                SUM(oi.price * oi.order_item_id) AS total_revenue,
                COUNT(oi.order_id) AS total_orders
            FROM order_items oi
            JOIN products pr ON oi.product_id = pr.product_id
            GROUP BY pr.product_id, pr.product_category
            ORDER BY total_revenue DESC
            LIMIT 10;
                    """

    df_top_products = pd.read_sql(query_top_products, engine)
    print("=== Top 10 Products by Revenue ===")
    print(df_top_products)
    print("\n")

        # ----------------------------
        # 3️⃣ Customer Order Frequency
        # ----------------------------
    query_customer_orders = """
                SELECT
            c.customer_id,
            c.customer_unique_id,
            COUNT(o.order_id) AS total_orders,
            SUM(p.payment_value) AS total_spent
        FROM customers c
        JOIN orders o ON c.customer_id = o.customer_id
        JOIN payments p ON o.order_id = p.order_id
        GROUP BY c.customer_id, c.customer_unique_id
        ORDER BY total_orders DESC
        LIMIT 10;

        """

    df_customer_orders = pd.read_sql(query_customer_orders, engine)
    print("=== Top 10 Customers by Number of Orders ===")
    print(df_customer_orders)

conn.close()
