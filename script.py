import psycopg2
import csv

conn = psycopg2.connect(host='localhost', database='north', user='postgres', password='54321')
count = 1
with conn.cursor() as cur:
    with open('customers_data.csv') as customers:
        customers_data = csv.DictReader(customers)
        for row in customers_data:
            cur.execute('INSERT INTO customers VALUES (%s, %s, %s)',
                        (row['customer_id'], row['company_name'], row['contact_name']))
    with open('employees_data.csv') as employees:
        employees_data = csv.DictReader(employees)
        for row in employees_data:
            cur.execute('INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)',
                        (row['first_name'], row['last_name'], row['title'], row['birth_date'], row['notes'], count))
            count += 1
    with open('orders_data.csv') as orders:
        orders_data = csv.DictReader(orders)
        for row in orders_data:
            cur.execute('INSERT INTO orders VALUES (%s, %s, %s, %s, %s)',
                        (row['order_id'], row['customer_id'], row['employee_id'], row['order_date'], row['ship_city']))

conn.commit()
conn.close()
