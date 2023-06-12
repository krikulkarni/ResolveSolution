import argparse
from typing import List, Dict, Any

import pandas as pd
import glob
import os
import json


def get_params() -> dict:
    parser = argparse.ArgumentParser(description='DataTest')
    parser.add_argument('--customers_location', required=False, default="./input_data/starter/customers.csv")
    parser.add_argument('--products_location', required=False, default="./input_data/starter/products.csv")
    parser.add_argument('--transactions_location', required=False, default="./input_data/starter/transactions/")
    parser.add_argument('--output_location', required=False, default="./output_data/outputs/")
    return vars(parser.parse_args())


# Function used to check whether file is empty or not
def is_csv_empty(file_path):
    df = pd.read_csv(file_path)
    return df.empty


# Function used to read the json data from multiple directory in normalize form
def get_json_data(transaction_directory) -> list[dict[str, Any]]:
    date_folders = glob.glob(
        os.path.normpath(os.path.join(transaction_directory, 'd=*')))  # Get the path of each date folder
    processed_data = []

    for date_folder in date_folders:
        # Get the path of the JSON files within the date folder
        json_files = glob.glob(os.path.normpath(os.path.join(date_folder, '*.json')))

        # Iterate over the JSON files
        for json_file in json_files:
            with open(json_file, 'r') as f:
                content: str = f.read()

                # Split the content by newlines
                lines = content.splitlines()
                for line in lines:
                    # We need to remove unexpected character from each line
                    if not line.strip():
                        continue
                    try:
                        # Read the JSON data
                        json_data = json.loads(line)
                        customer_id = json_data['customer_id']
                        date_of_purchase = json_data['date_of_purchase']
                        # Normalizing the product data using for loop
                        for item in json_data['basket']:
                            product_id = item['product_id']
                            price = item['price']
                            processed_data.append({'customer_id': customer_id, 'date_of_purchase': date_of_purchase,
                                                   'product_id': product_id, 'price': price})
                    except json.JSONDecodeError as e:
                        # Handle any decoding errors
                        print(f"Error decoding JSON: {e}")
    # Returning the processed data
    return processed_data


# Function used to read the data from csv
def get_csv_data(file_location):
    df_products = pd.read_csv(file_location)
    df_products = df_products.dropna()
    return df_products


# Function used to Merge the JSON, Customer and Product data
def get_merge_data(json_df, df_customers, df_products):
    # Merging the JSON and Customer data on the basis of customer_id and using left Join
    customers_merged_data = pd.merge(json_df, df_customers, on='customer_id', how='left')
    # Merging the JSON and Product data on the basis of product_id and using left Join
    final_merged_data = pd.merge(customers_merged_data, df_products, on='product_id', how='left')
    # Cal purchase_count based on product_id
    purchase_count = final_merged_data.groupby('product_id').size().reset_index(name='purchase_count')
    # Merging the Purchase_count column with final data
    output_data = final_merged_data[['customer_id', 'loyalty_score', 'product_id', 'product_category']].merge(
        purchase_count, on='product_id')
    # Returning the final data
    return output_data


# Function used to write the data in CSV
def write_csv(output_data, output_directory):
    # Creating the output directory
    os.makedirs(output_directory, exist_ok=True)
    output_file = os.path.join(output_directory, "output.csv")
    # Writing the data in the CSV file
    output_data.to_csv(output_file, index=False)
    return is_csv_empty(output_file)


def main():
    # Calling the get_param() to fetch all the parameters
    params = get_params()
    products_directory = params.get('products_location')
    customers_directory = params.get('customers_location')
    output_directory = params.get('output_location')
    transaction_directory = params.get('transactions_location')

    # Read the CSV file into a DataFrame
    df_products = get_csv_data(products_directory)
    df_customers = get_csv_data(customers_directory)

    # Calling the function to get the json data
    json_df = pd.DataFrame(get_json_data(transaction_directory))

    # Calling the function to merger the JSON, Customer, Product data
    output_data = get_merge_data(json_df, df_customers, df_products)

    # Calling the function to write the data in CSV file and checking whether the file is generated or not
    if write_csv(output_data, output_directory):
        print('Output file is empty')
    else:
        print('Output file has been generated successfully')


if __name__ == "__main__":
    main()
