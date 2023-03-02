import csv
import sqlite3
import random

# Define the filenames and column mappings for each file

files = [
    {'filename': 'olist_customers_dataset.csv', 'table': 'Customers', 'columns': {'customer_id': 0, 'customer_postal_code': 2}, 'rename_columns': {'customer_zip_code_prefix': 'customer_postal_code'}, 'row_count': 20000},
    {'filename': 'olist_sellers_dataset.csv', 'table': 'Sellers', 'columns': {'seller_id': 0, 'seller_postal_code': 1}, 'row_count': 750},
    {'filename': 'olist_orders_dataset.csv', 'table': 'Orders', 'columns': {'order_id': 0, 'customer_id': 1}, 'row_count': 20000},
    {'filename': 'olist_order_items_dataset.csv', 'table': 'Order_items', 'columns': {'order_id': 0, 'order_item_id': 1, 'product_id': 2, 'seller_id': 3}, 'row_count': 4000}
]

# Connect to the SQLite database
conn = sqlite3.connect('A3Medium.db')
c = conn.cursor()

# Loop through each file and insert the selected columns into the database
for file in files:
    # Create the table in the database
    if file['table'] == 'Customers':
        c.execute('''CREATE TABLE Customers
                     (customer_id text PRIMARY KEY, customer_postal_code integer)''')
    elif file['table'] == 'Sellers':
        c.execute('''CREATE TABLE Sellers
                     (seller_id text PRIMARY KEY, seller_postal_code integer)''')
    elif file['table'] == 'Orders':
        c.execute('''CREATE TABLE Orders
                     (order_id text PRIMARY KEY, customer_id text,
                      FOREIGN KEY(customer_id) REFERENCES Customers(customer_id))''')
    elif file['table'] == 'Order_items':
        c.execute('''CREATE TABLE Order_items
                     (order_id text, order_item_id integer, product_id text, seller_id text,
                      PRIMARY KEY(order_id, order_item_id, product_id, seller_id),
                      FOREIGN KEY(seller_id) REFERENCES Sellers(seller_id),
                      FOREIGN KEY(order_id) REFERENCES Orders(order_id))''')

    # Open the CSV file and read in the columns
    with open(file['filename'], 'r', encoding='utf-8') as csvfile:
        csvreader = csv.reader(csvfile)
        headers = next(csvreader)  # skip the header row

        # Choose the specified columns
        # Choose the specified columns and rename them if necessary
        selected_columns = []
        for column, index in file['columns'].items():
            if column in file.get('rename_columns', {}):
                selected_columns.append(file['rename_columns'][column])
            else:
                selected_columns.append(column)


        # Collect all rows in a list and shuffle it
        all_rows = [row for row in csvreader]
        random.shuffle(all_rows)

        # Select row_count number of rows without duplicates
        selected_rows = []
        for row in all_rows:
            if len(selected_rows) >= file['row_count']:
                break
            values = [row[index] for column, index in file['columns'].items()]
            if values not in selected_rows:
                selected_rows.append(values)

        # Insert the selected rows into the database
        for row in selected_rows:
            c.execute("INSERT INTO {} ({}) VALUES ({})".format(file['table'], ','.join(selected_columns), ','.join(['?']*len(row))), row)

# Commit the changes and close the database connection
conn.commit()
conn.close()
