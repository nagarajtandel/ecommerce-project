from flask import Flask, render_template, request
import pandas as pd
from sqlalchemy import create_engine

app = Flask(__name__)

# MySQL connection
engine = create_engine('mysql+mysqlconnector://root:Fresher%4015@localhost:3306/ecommerce')

# Home page
@app.route('/')
def home():
    query = "SELECT product_id, product_category FROM products LIMIT 10;"
    df_products = pd.read_sql(query, engine)
    return render_template('index.html', products=df_products.to_dict(orient='records'))

# Products page
@app.route('/products')
def products():
    search = request.args.get('search', '')
    if search:
        query = f"SELECT * FROM products WHERE product_category LIKE '%{search}%' OR product_name_length LIKE '%{search}%';"
    else:
        query = "SELECT * FROM products LIMIT 50;"
    df_products = pd.read_sql(query, engine)
    return render_template('products.html', products=df_products.to_dict(orient='records'), search=search)

# Customers page
@app.route('/customers')
def customers():
    query = """
            SELECT c.customer_id, c.customer_unique_id, COUNT(o.order_id) AS total_orders,
            SUM(p.payment_value) AS total_spent
        FROM customers c
        JOIN orders o ON c.customer_id = o.customer_id
        JOIN payments p ON o.order_id = p.order_id
        GROUP BY c.customer_id, c.customer_unique_id
        ORDER BY total_orders DESC
        LIMIT 20;

    """
    df_customers = pd.read_sql(query, engine)
    return render_template('customers.html', customers=df_customers.to_dict(orient='records'))

if __name__ == '__main__':
    app.run(debug=True)
