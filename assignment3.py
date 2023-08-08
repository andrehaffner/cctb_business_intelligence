from pandas import Dataframe
import pymongo
import boto3


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Functions ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ #

def transform_records(table_id, records):
    if table_id == 'Customer':
        transformed = []
        for rec in records:
            customer_id = rec['customer_id']
            name = rec['name']
            email = rec['email']
            address = rec['address']
            phone = rec['phoneNumber']
            full_name = f"{name} ({customer_id})"
            contact_info = {'email': email, 'address': address, 'phoneNumber': phone}
            transformed.append({'customer_id': customer_id, 'full_name': full_name, 'contact_info': contact_info})
        return transformed
    elif table_id == 'Subscription':
        transformed = []
        for rec in records:
            sub_id = rec['subscriptionID']
            price = rec['price']
            sub_type = rec['subscriptionType']
            sub_date = rec['subscriptionDate']
            cust_id = rec['customer_id']
            pay_id = rec['paymentID']
            transformed.append({'subscription_id': sub_id, 'price': price, 'subscription_type': sub_type,
                                'subscription_date': sub_date, 'customer': cust_id, 'payment': pay_id})
        return transformed
    elif table_id == 'Genre':
        transformed = []
        for rec in records:
            genre_id = rec['genreId']
            description = rec['description']
            transformed.append({'genre_id': genre_id, 'description': description})
        return transformed
    elif table_id == 'Movie':
        return records
    elif table_id == 'Payment':
        transformed = []
        for rec in records:
            pay_id = rec['paymentID']
            pay_method = rec['paymentMethod']
            pay_date = rec['paymentDate']
            amount = rec['amount']
            cust_id = rec['customer_id']
            transformed.append({'payment_id': pay_id, 'payment_method': pay_method, 'payment_date': pay_date,
                                'amount': amount, 'customer': cust_id})
        return transformed

    else:
        print("Unknown table")
        return Dataframe()


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Code ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ #

conn = 'string_to_connect'
client = pymongo.MongoClient(conn)

access_id = 'access_id'
secret_key = 'super_secret_key'
dynamoDB = boto3.client('dynamodb',
                        region_name='eu-north-1',
                        aws_access_key_id=access_id,
                        aws_secret_access_key=secret_key)


table_names = ['Customer', 'Subscription', 'Genre', 'Movie', 'Payment']

records_from_stream = {
    'Customer': [
        {"customer_id": 1, "name": "John Doe", "email": "john.doe@example.com", "address": "123 Main St",
         "phoneNumber": "123-456-7890"},
        {"customer_id": 2, "name": "Jane Smith", "email": "jane.smith@example.com", "address": "456 Oak St",
         "phoneNumber": "987-654-3210"}
    ],
    'Subscription': [
        {"subscriptionID": 1, "price": 9.99, "subscriptionType": "Monthly", "subscriptionDate": "2023-07-15",
         "customer_id": 1, "paymentID": 101},
        {"subscriptionID": 2, "price": 29.99, "subscriptionType": "Annual", "subscriptionDate": "2023-07-18",
         "customer_id": 2, "paymentID": 102}
    ],
    'Genre': [
        {"genreId": 1, "description": "Action"},
        {"genreId": 2, "description": "Comedy"}
    ],
    'Movie': [
        {"movieID": 1, "title": "Movie A", "release": "2023-07-20", "rating": 7.5, "genreId": 1,
         "subscriptionId": 1},
        {"movieID": 2, "title": "Movie B", "release": "2023-07-25", "rating": 8.2, "genreId": 2,
         "subscriptionId": 2}
    ],
    'Payment': [
        {"paymentID": 101, "paymentMethod": "Credit Card", "paymentDate": "2023-07-15", "amount": 9.99,
         "customer_id": 1},
        {"paymentID": 102, "paymentMethod": "PayPal", "paymentDate": "2023-07-18", "amount": 29.99,
         "customer_id": 2}
    ]
}

for table_name in table_names:
    try:
        db = client['your_mongodb_database']
        collection = db[table_name]
        records = transform_records(table_name, records_from_stream[table_name])
        collection.insert_many(records)
        print(f"Sync for table '{table_name}' completed.")
    except Exception as e:
        print(f"Sync error for table '{table_name}': {e}")

