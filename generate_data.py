import psycopg2
from faker import Faker

faker = Faker()


class DatabaseManager:
    def __init__(self, host, database, user, password, port):
        self.conn = psycopg2.connect(host=host, database=database, user=user, password=password, port=port)
        self.cursor = self.conn.cursor()

    def create_table(self, table_name, columns):
        columns_with_types = ', '.join([f'{col} {dtype}' for col, dtype in columns.items()])
        query = f'''
            CREATE TABLE IF NOT EXISTS {table_name}(
                {columns_with_types}
            );
            ALTER TABLE {table_name} REPLICA IDENTITY FULL;
        '''  # add replication of data before changes to see at debezium
        self.cursor.execute(query)
        self.conn.commit()

    def insert_data(self, table_name, data):
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['%s'] * len(data))
        query = f'INSERT INTO {table_name} ({columns}) VALUES ({placeholders})'
        self.cursor.execute(query, list(data.values()))
        self.conn.commit()

    def close(self):
        self.cursor.close()
        self.conn.close()


class DataGenerator:
    def __init__(self):
        self.faker = Faker()

    def generate_customer(self):
        user = self.faker.simple_profile()
        return {
            "id": self.faker.uuid4(),
            "user_name": user['username'],
            "name": user['name'],
            "sex": user['sex'],
            "email": user['mail'],
            "birthdate": user['birthdate'],
            "create_date": self.faker.date_this_decade()
        }

    def generate_order(self, customer_id):
        return {
            "id": self.faker.uuid4(),
            "customer_id": customer_id,
            "order_date": self.faker.date_time_this_year(),
            "status": self.faker.random_element(elements=("Pending", "Shipped", "Delivered", "Cancelled"))
        }

    def generate_address(self, customer_id):
        return {
            "id": self.faker.uuid4(),
            "customer_id": customer_id,
            "address": self.faker.street_address(),
            "city": self.faker.city(),
            "state": self.faker.state(),
            "zip_code": self.faker.zipcode(),
            "country": self.faker.country()
        }

    def generate_item(self):
        return {
            "id": self.faker.uuid4(),
            "name": self.faker.word(),
            "description": self.faker.text(),
            "price": self.faker.random_number(digits=5) / 100,
            "create_date": self.faker.date_this_decade()
        }

    def generate_order_details(self, order_id, item_id):
        return {
            "id": self.faker.uuid4(),
            "order_id": order_id,
            "item_id": item_id,
            "quantity": self.faker.random_int(min=1, max=10),
            "price": self.faker.random_number(digits=5) / 100
        }


def main():
    db_manager = DatabaseManager(host='localhost', database='financial_db', user='postgres', password='postgres', port='5432')
    data_generator = DataGenerator()

    # Define table schemas
    tables = {
        'customer': {
            'id': 'UUID PRIMARY KEY',
            'user_name': 'VARCHAR(255)',
            'name': 'VARCHAR(255)',
            'sex': 'CHAR(1)',
            'email': 'VARCHAR(255)',
            'birthdate': 'DATE',
            'create_date': 'TIMESTAMP'
        },
        'orders': {
            'id': 'UUID PRIMARY KEY',
            'customer_id': 'UUID',
            'order_date': 'TIMESTAMP',
            'status': 'VARCHAR(50)',
            'FOREIGN KEY (customer_id)': 'REFERENCES customer(id)'
        },
        'addresses': {
            'id': 'UUID PRIMARY KEY',
            'customer_id': 'UUID',
            'address': 'VARCHAR(255)',
            'city': 'VARCHAR(255)',
            'state': 'VARCHAR(255)',
            'zip_code': 'VARCHAR(10)',
            'country': 'VARCHAR(255)',
            'FOREIGN KEY (customer_id)': 'REFERENCES customer(id)'
        },
        'items': {
            'id': 'UUID PRIMARY KEY',
            'name': 'VARCHAR(255)',
            'description': 'TEXT',
            'price': 'DECIMAL(10, 2)',
            'create_date': 'TIMESTAMP'
        },
        'order_details': {
            'id': 'UUID PRIMARY KEY',
            'order_id': 'UUID',
            'item_id': 'UUID',
            'quantity': 'INTEGER',
            'price': 'DECIMAL(10, 2)',
            'FOREIGN KEY (order_id)': 'REFERENCES orders(id)',
            'FOREIGN KEY (item_id)': 'REFERENCES items(id)'
        }
    }

    # Create tables
    for table_name, columns in tables.items():
        db_manager.create_table(table_name, columns)

    # Generate and insert data
    customer = data_generator.generate_customer()
    db_manager.insert_data('customer', customer)

    address = data_generator.generate_address(customer["id"])
    db_manager.insert_data('addresses', address)

    order = data_generator.generate_order(customer["id"])
    db_manager.insert_data('orders', order)

    item = data_generator.generate_item()
    db_manager.insert_data('items', item)

    order_details = data_generator.generate_order_details(order["id"], item["id"])
    db_manager.insert_data('order_details', order_details)

    db_manager.close()


if __name__ == "__main__":
    main()
