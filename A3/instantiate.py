import csv
import sqlite3
import random

def main():
    dictionary = {'A3Small.db': [10000, 500, 10000, 2000], 'A3Medium.db': [20000, 750, 20000, 1000], 'A3Large.db':[33000, 1000, 33000, 4000]}

    for key, value in dictionary.items():
        conn = sqlite3.connect(key)
        c = conn.cursor()
        
        files = create_files(value)
        for file in files:
            create_table(c, file)
            select_rows(c, file)

        conn.commit()
        conn.close()

def create_files(args):
    files = [
        {'filename': 'olist_customers_dataset.csv', 'table': 'Customers', 'columns': {'customer_id': 0, 'customer_postal_code': 2}, 'rename_columns': {'customer_zip_code_prefix': 'customer_postal_code'}, 'row_count': args[0]},
        {'filename': 'olist_sellers_dataset.csv', 'table': 'Sellers', 'columns': {'seller_id': 0, 'seller_postal_code': 1}, 'row_count': args[1]},
        {'filename': 'olist_orders_dataset.csv', 'table': 'Orders', 'columns': {'order_id': 0, 'customer_id': 1}, 'row_count': args[2]},
        {'filename': 'olist_order_items_dataset.csv', 'table': 'Order_items', 'columns': {'order_id': 0, 'order_item_id': 1, 'product_id': 2, 'seller_id': 3}, 'row_count': args[3]}
    ]

    return files


def create_table(c, file):
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

def select_rows(c, file):
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

def Redefine():

    # Establish a connection to the database
    conn = sqlite3.connect('example.db')

    # Create a cursor object
    cur = conn.cursor()

    # Redefine primary key for the Customers table
    cur.execute('CREATE TABLE Customers_new (customer_id text, customer_postal_code integer, PRIMARY KEY(customer_id))')
    cur.execute('INSERT INTO Customers_new SELECT * FROM Customers')
    cur.execute('DROP TABLE Customers')
    cur.execute('ALTER TABLE Customers_new RENAME TO Customers')

    # Redefine primary key for the Sellers table
    cur.execute('CREATE TABLE Sellers_new (seller_id text, seller_postal_code integer, PRIMARY KEY(seller_id))')
    cur.execute('INSERT INTO Sellers_new SELECT * FROM Sellers')
    cur.execute('DROP TABLE Sellers')
    cur.execute('ALTER TABLE Sellers_new RENAME TO Sellers')

    # Commit the changes and close the cursor and connection
    conn.commit()
    cur.close()
    conn.close()


main()
