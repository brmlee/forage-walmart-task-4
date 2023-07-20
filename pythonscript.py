import sqlite3
import csv

conn = sqlite3.connect('shipment_database.db')
c = conn.cursor()

# Insert data from spreadsheet 0
with open('shipping_data_0.csv') as f:
    reader = csv.reader(f)
    next(reader) # Skip header row
    for row in reader:
        c.execute("INSERT INTO shipments VALUES (?,?,?,?,?,?,?)", row)

# Insert data from spreadsheets 1 and 2 
shipments = {}
with open('shipping_data_1.csv') as f:
    reader = csv.reader(f)
    next(reader)
    for row in reader:
        shipment_id = row[0]
        product = row[1]
        on_time = row[2] == 'true'
        
        if shipment_id not in shipments:
            shipments[shipment_id] = []
        shipments[shipment_id].append((product, on_time))

with open('shipping_data_2.csv') as f:
    reader = csv.reader(f)
    next(reader)
    for row in reader:
        shipment_id = row[0]
        origin = row[1]
        destination = row[2]
        driver = row[3]
        
        for product, on_time in shipments[shipment_id]:
            quantity = len(shipments[shipment_id])
            c.execute("""
                INSERT INTO shipments 
                VALUES (?,?,?,?,?,?,?)
            """, (shipment_id, origin, destination, driver, 
                   product, on_time, quantity))

conn.commit()
conn.close()