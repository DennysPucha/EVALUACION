from faker import Faker
import random
import csv

faker = Faker()
productos = []

for i in range(1, 1000001):
    producto = {
        "id": str(i),
        "nombre": faker.word().capitalize() + " " + faker.company(),
        "precio": round(random.uniform(10, 3000), 2)
    }
    productos.append(producto)

with open("productos.csv", "w", newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(["id", "nombre", "precio"])  # encabezado
    for producto in productos:
        writer.writerow([producto["id"], producto["nombre"], producto["precio"]])
