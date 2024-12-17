import os
import requests
import pandas as pd
from dotenv import load_dotenv
from itertools import combinations
from collections import Counter


def retrieve_customers():
    """Function to retrieve and display the first 10 customers from the Square production environment."""
    response = requests.get(url_customers, headers=headers)

    if response.status_code == 200:
        result = response.json()
        print('\n'.join(str(result).splitlines()[:10]))

        if 'customers' in result:
            for idx, customer in enumerate(result['customers']):
                if idx == 10:  # Stop after 10 customers
                    break
                print(f"Customer ID: {customer['id']}, Name: {customer.get('given_name', '')} {customer.get('family_name', '')}")
    else:
        print(f"Error retrieving customers: {response.status_code}")
        print(response.text)

    return result


def retrieve_payments():
    """Function to retrieve and display the first 10 payments from the Square production environment."""
    response = requests.get(url_payments, headers=headers)

    if response.status_code == 200:
        result = response.json()
        print('\n'.join(str(result).splitlines()[:10]))

        if 'payments' in result:
            for idx, payment in enumerate(result['payments']):
                if idx == 10:  # Stop after 10 payments
                    break
                amount = payment['amount_money']['amount'] / 100  # Convert to dollars if needed
                currency = payment['amount_money']['currency']
                print(f"Payment ID: {payment['id']}, Amount: {amount} {currency}, Status: {payment['status']}")
    else:
        print(f"Error retrieving payments: {response.status_code}")
        print(response.text)

    return result


def get_orders_from_payment(payment_id):
    """Retrieve the order associated with a given payment ID."""
    # Make a GET request to retrieve the payment details, which includes the order_id
    payment_response = requests.get(f'{url_payments}/{payment_id}', headers=headers)

    if payment_response.status_code == 200:
        payment_data = payment_response.json()
        order_id = payment_data['payment']['order_id']  # Get the associated order_id

        # Retrieve the actual order using the order_id
        order_response = requests.get(f'{url_orders}/{order_id}', headers=headers)

        if order_response.status_code == 200:
            order_data = order_response.json()

            # Print out the items purchased in the order
            if 'line_items' in order_data['order']:
                for item in order_data['order']['line_items']:
                    print(f"Item Name: {item['name']}, Variation: {item['variation_name']}, Quantity: {item['quantity']}")
        else:
            print(f"Error retrieving order: {order_response.status_code}")
            print(order_response.text)
    else:
        print(f"Error retrieving payment: {payment_response.status_code}")
        print(payment_response.text)

    return order_data


def retrieve_all_orders(location_id, begin_time=None, end_time=None):
    """Function to retrieve all orders for a specific location."""
    orders = []
    cursor = None

    while True:
        # Define the request body inside the loop
        body = {
            "location_ids": [location_id],
            "limit": 500  # Maximum allowed by the API per request
        }

        # Optional date filtering
        if begin_time and end_time:
            body["query"] = {
                "filter": {
                    "date_time_filter": {
                        "created_at": {
                            "start_at": begin_time,
                            "end_at": end_time
                        }
                    }
                }
            }

        # Include the cursor if it's not None
        if cursor:
            body['cursor'] = cursor

        # Send the POST request to retrieve orders
        response = requests.post(url_orders_search, headers=headers, json=body)

        if response.status_code == 200:
            result = response.json()
            orders.extend(result.get('orders', []))
            print(orders[-1])

            # Update the cursor
            cursor = result.get('cursor')
            if not cursor:
                break  # No more pages
        else:
            print(f"Error retrieving orders: {response.status_code}")
            print(response.text)
            break  # Exit the loop on error

    return orders


def retrieve_all_orders_original(location_id, begin_time=None, end_time=None):
    """Function to retrieve all orders for a specific location."""
    # Define the request body with optional date filtering
    body = {
        "location_ids": [location_id],  # Specify the location ID to search orders for
        "query": {}
    }

    # Optional date filtering
    if begin_time and end_time:
        body["query"]["filter"] = {
            "date_time_filter": {
                "created_at": {
                    "start_at": begin_time,
                    "end_at": end_time
                }
            }
        }

    # Send the POST request to retrieve orders
    response = requests.post(url_orders_search, headers=headers, json=body)

    if response.status_code == 200:
        result = response.json()
        orders = result.get('orders', [])

        # Print order details
        for order in orders:
            print(f"Order ID: {order['id']}, Created At: {order['created_at']}, Status: {order['state']}")

        return orders

    else:
        print(f"Error retrieving orders: {response.status_code}")
        print(response.text)


def extract_order_items(orders):
    """Extract items from orders and return a list of transactions."""
    transactions = []
    for order in orders:
        line_items = order.get('line_items', [])
        items_in_order = []
        for item in line_items:
            item_name = item['item_name']
            quantity = int(item.get('quantity', '1'))
            items_in_order.extend([item_name] * quantity)
        if items_in_order:
            transactions.append(items_in_order)
    return transactions


def orders_to_dataframe(orders):
    """Convert a list of orders to a pandas DataFrame."""
    records = []
    for order in orders:
        order_id = order.get('id')
        location_id = order.get('location_id')
        created_at = order.get('created_at')
        updated_at = order.get('updated_at')
        state = order.get('state')

        # Check if 'line_items' exists and is not empty
        line_items = order.get('line_items', [])
        if line_items:
            for item in line_items:
                record = {
                    'order_id': order_id,
                    'location_id': location_id,
                    'created_at': created_at,
                    'updated_at': updated_at,
                    'state': state,
                    'item_id': item.get('catalog_object_id'),
                    'item_name': item.get('name'),
                    'variation_name': item.get('variation_name'),
                    'quantity': item.get('quantity'),
                    'base_price': item.get('base_price_money', {}).get('amount', 0) / 100,
                    'total_money': item.get('total_money', {}).get('amount', 0) / 100,
                }
                records.append(record)
        else:
            # Handle orders without line items if needed
            # For example, log them or store basic info
            record = {
                'order_id': order_id,
                'location_id': location_id,
                'created_at': created_at,
                'updated_at': updated_at,
                'state': state,
                'item_id': None,
                'item_name': None,
                'variation_name': None,
                'quantity': None,
                'base_price': None,
                'total_money': order.get('total_money', {}).get('amount', 0) / 100,
            }
            records.append(record)
    df = pd.DataFrame(records)
    return df


# Get the root project directory
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

# Load the .env file from the root directory
load_dotenv(os.path.join(project_root, '.env'))

# sandbox_access_token = os.getenv('SANDBOX_ACCESS_TOKEN')
production_access_token = os.getenv('PRODUCTION_ACCESS_TOKEN')

# URLs for the production environment
url_customers = 'https://connect.squareup.com/v2/customers'
url_payments = 'https://connect.squareup.com/v2/payments'
url_orders = 'https://connect.squareup.com/v2/orders'
url_orders_search = 'https://connect.squareup.com/v2/orders/search'

# Define the headers with the Bearer token for authentication
headers = {
    'Authorization': f'Bearer {production_access_token}',
    'Content-Type': 'application/json'
}

# Retrieve all orders
location_id = 'ZE934VV8RCWGF'
begin_time = '2024-09-01T00:00:00Z'
end_time = '2024-09-30T23:59:59Z'
orders = retrieve_all_orders(location_id, begin_time, end_time)

# Extract items from orders
transactions = extract_order_items(orders)

# Create a DataFrame of transactions
df_transactions = pd.DataFrame({'items': transactions})

# Display the first few transactions
print(df_transactions.head())

##### Pairs #####
# Count item pairs
item_pairs = Counter()
for items in transactions:
    # Get all unique combinations of items in a transaction
    for pair in combinations(set(items), 2):
        item_pairs[pair] += 1

# Convert to DataFrame
df_item_pairs = pd.DataFrame(item_pairs.items(), columns=['pair', 'count'])

# Sort by count in descending order
df_item_pairs = df_item_pairs.sort_values(by='count', ascending=False)

# Display the top item pairs
print(df_item_pairs.head(20))

# Older
result_customer = retrieve_customers()
result_payment = retrieve_payments()
payment_id = '9cz2SGyoUPxNucaG0pwmWeLavaB'
result_order = get_orders_from_payment(payment_id)

pass

# TODO
# Get all orders > 0 USD

# Write
# Convert orders to DataFrame

# Write to CSV
df_orders = orders_to_dataframe(orders)
output_path = r'C:\Users\samea\PycharmProjects\WaypointCoffeeSquare\orders_90k_2024.csv'
df_orders.to_csv(output_path, index=False, mode='w', encoding='utf-8-sig')

## Put in pandas dataframe with these tables:
# Transaction table:
# order_id | location_id | customer_id | created at | dollars | item_id | quantity_purchased
# Item table:
# item_id | item_name | item_category

for idx, order in enumerate(result_order['order']['line_items']):
    if idx == 2:  # Stop after 10 payments
        break
    print(order)

for idx, payment in enumerate(result_payment['payments']):
    if idx == 2:  # Stop after 10 payments
        break
    amount = payment['amount_money']['amount'] / 100  # Convert to dollars if needed
    currency = payment['amount_money']['currency']
    print(payment)

a = 1
