import sqlite3
import time
import matplotlib
matplotlib.use('Agg')
from matplotlib import pyplot as plt
import numpy as np
import sys

def uninformed(c):
    c.execute('PRAGMA foreign_keys = OFF;')
    c.execute('BEGIN TRANSACTION;')
    c.execute('CREATE TABLE Customers(customer_id text, customer_postal_code integer);')
    c.execute('CREATE TABLE Sellers(seller_id text, seller_postal_code integer);')
    c.execute('CREATE TABLE Orders(order_id text, customer_id text);')
    c.execute('CREATE TABLE Order_items(order_id text, order_item_id integer, product_id text, seller_id text);')
    c.execute('COMMIT;')
    c.execute('PRAGMA automatic_index = 0;')

def self_optimized(c):
    c.execute('PRAGMA foreign_keys = ON;')
    c.execute('BEGIN TRANSACTION;')
    c.execute('CREATE TABLE Customers(customer_id text PRIMARY KEY, customer_postal_code integer);')
    c.execute('CREATE TABLE Sellers(seller_id text PRIMARY KEY, seller_postal_code integer);')
    c.execute('CREATE TABLE Orders(order_id text PRIMARY KEY, customer_id text, FOREIGN KEY(customer_id) REFERENCES Customers(customer_id));')
    c.execute('CREATE TABLE Order_items(order_id text, order_item_id integer, product_id text, seller_id text, PRIMARY KEY(order_id, order_item_id, product_id, seller_id), FOREIGN KEY(seller_id) REFERENCES Sellers(seller_id), FOREIGN KEY(order_id) REFERENCES Orders(order_id));')
    c.execute('COMMIT;')
    c.execute('PRAGMA automatic_index = 1;')

def user_optimized(c):
    c.execute('PRAGMA foreign_keys = ON;')
    c.execute('BEGIN TRANSACTION;')
    c.execute('CREATE TABLE Customers(customer_id text PRIMARY KEY, customer_postal_code integer);')
    c.execute('CREATE TABLE Sellers(seller_id text PRIMARY KEY, seller_postal_code integer);')
    c.execute('CREATE TABLE Orders(order_id text PRIMARY KEY, customer_id text, FOREIGN KEY(customer_id) REFERENCES Customers(customer_id));')
    c.execute('CREATE TABLE Order_items(order_id text, order_item_id integer, product_id text, seller_id text, PRIMARY KEY(order_id, order_item_id, product_id, seller_id), FOREIGN KEY(seller_id) REFERENCES Sellers(seller_id), FOREIGN KEY(order_id) REFERENCES Orders(order_id));')
    c.execute('CREATE INDEX idx_customer_postal_code ON Customers(customer_postal_code);')
    c.execute('CREATE INDEX idx_seller_postal_code ON Sellers(seller_postal_code);')
    c.execute('CREATE INDEX idx_order_customer ON Orders(customer_id);')
    c.execute('CREATE INDEX idx_order_seller ON Order_items(seller_id);')
    c.execute('COMMIT;')
    c.execute('PRAGMA automatic_index = 1;')

def main():
    databases = ['A3Small.db', 'A3Medium.db', 'A3Large.db']

    uninformed_arr = []
    self_optimized_arr = []
    user_optimized_arr = []


    for database in databases:
       
        
        conn = sqlite3.connect(database)
        c = conn.cursor()
        query1 = """
        SELECT customer_postal_code 
        FROM Customers 
        ORDER BY RANDOM() 
        LIMIT 1
        """
        c.execute(query1)
        customer_postal_code = str(c.fetchone()[0])
        query2 = """
        SELECT COUNT(DISTINCT o.order_id)
        FROM Customers c, Orders o, Order_items oi
        WHERE c.customer_id = o.customer_id AND 
        o.order_id = oi.order_id AND
        c.customer_postal_code = ?
        GROUP BY c.customer_postal_code
        HAVING COUNT(*) > 1;
        """
        uninformed(c)
        start_time = time.time()
        for i in range(50):
            c.execute(query2, (customer_postal_code,))
        end_time = time.time()
        total_time1 = end_time - start_time
        uninformed_arr.append(total_time1)
        

        self_optimized(c)
        start_time = time.time()
        for i in range(50):
            c.execute(query2, (customer_postal_code,))
        end_time = time.time()
        total_time2 = end_time - start_time
        self_optimized_arr.append(total_time2)

        
        user_optimized(c)
        start_time = time.time()
        for i in range(50):
            c.execute(query2, (customer_postal_code,))
        end_time = time.time() 
        total_time3 = end_time - start_time
        user_optimized_arr.append(total_time3)

        conn.close()



    x = ['SmallDB', 'MediumDB', 'LargeDB']

    uninformed_data = np.array(uninformed_arr)
    self_optimized_data = np.array(self_optimized_arr)
    user_optimized_data = np.array(user_optimized_arr)

    plt.bar(x, uninformed_data, color='gold')
    plt.bar(x, self_optimized_data, bottom=uninformed_data, color='lightskyblue')
    plt.bar(x, user_optimized_data, bottom=uninformed_data+self_optimized_data, color='tomato')
    plt.title('Query 1 (runtime in ms)')
    plt.legend(["uninformed", "self optimized", "user optimized"])
    plt.show()
    plt.savefig('image.png')
        
main()
