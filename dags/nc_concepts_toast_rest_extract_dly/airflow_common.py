from airflow.hooks.base import BaseHook
import logging
import requests


def get_toast_bearer_token(**kwargs):
    logging.info("Getting Toast Bearer Token")
    toast_conn_id = kwargs['toast_conn_id']
    restaurant_external_id = kwargs['restaurant_external_id']
    auth_url = "https://ws-api.toasttab.com/authentication/v1/authentication/login"
    conn = BaseHook.get_connection(toast_conn_id)
    client_id = conn.login
    client_secret = conn.password
    logging.info(
        "Inital Arguments: \n"
        f"toast_conn_id: {toast_conn_id}, \n"
        f"restaurant_external_id: {restaurant_external_id}, \n"
        f"auth_url: {auth_url}, \n"
        f"client_id: {client_id}"
    )
    headers = {'Content-Type': 'application/json'}
    auth_payload = {
        'clientId': client_id,
        'clientSecret': client_secret,
        'userAccessType': 'TOAST_MACHINE_CLIENT'
    }
    auth_response = requests.post(auth_url, json=auth_payload, headers=headers)
    if auth_response.status_code == 200:
        auth_data = auth_response.json()
        bearer_token = auth_data.get('token', {}).get('accessToken')
        if not bearer_token:
            raise Exception("Failed to retrieve access token from authentication response.")
        return bearer_token
    else:
        raise Exception(f"Failed to Authenticate. Status code: {auth_response.status_code}. API Exception Response: {auth_response.text}")



def get_and_load_toast_data(**kwargs):
    logging.info("Getting and Loading Toast Data")
    mysql_conn_id = kwargs['mysql_conn_id']
    api_url = kwargs['api_url']
    restaurant_external_id = kwargs['restaurant_external_id']
    restaurant_name_simple = kwargs['restaurant_name_simple']
    restaurant_name = kwargs['restaurant_name']
    restaurant_address = kwargs['restaurant_address']
    table_load_info = kwargs['table_load_info']
    api_caller_db_loader_class = kwargs['api_caller_db_loader_class']
    bearer_token = kwargs['ti'].xcom_pull(task_ids=f'{restaurant_name_simple}_get_bearer_token', key='return_value')
    logging.info(
        "Inital Arguments: \n"
        f"mysql_conn_id: {mysql_conn_id}, \n"
        f"api_url: {api_url}, \n"
        f"restaurant_external_id: {restaurant_external_id}, \n"
        f"restaurant_name_simple: {restaurant_name_simple}, \n"
        f"restaurant_name: {restaurant_name}, \n"
        f"restaurant_address: {restaurant_address}, \n"
        f"table_load_info: {table_load_info}, \n"
        f"api_caller_db_loader_class: {api_caller_db_loader_class}, \n"
        f"bearer_token: {bearer_token}"
    )

    # Generate API Headers
    headers = {
        "Authorization": f"Bearer {bearer_token}",
        "Toast-Restaurant-External-ID": restaurant_external_id
    }
    logging.info(f"Generated Headers: {headers}")

    # Initialize API Caller and DB Loader
    api_caller_db_loader = api_caller_db_loader_class(api_url, headers, restaurant_external_id, restaurant_name, restaurant_address)

    # Call the API
    response = api_caller_db_loader.call_api()
    logging.info(f"API Response: {response}")

    # Process Data
    processed_data = api_caller_db_loader.process_api_response(response, table_load_info)
    logging.info(f"Processed Data: {processed_data}")

    # Load Into MySQL
    api_caller_db_loader.load_into_mysql(mysql_conn_id, processed_data, table_load_info)

    return "data_loaded_successfully"

