from dateutil import parser
from mysql.connector import Error
# import mysql.connector
from airflow.providers.mysql.hooks.mysql import MySqlHook
import requests
import logging


def rename_key(obj, old, new):
    if old in obj:
        obj[new] = obj.pop(old)

def to_mysql_datetime(dt_str: str) -> str:
    if dt_str is None:
        return None 
    dt = parser.parse(dt_str)
    return dt.strftime("%Y-%m-%d %H:%M:%S")



class APICalllerDBLoader:
    
    def __init__(self, api_url, headers, restaurant_external_id, restaurant_name, restaurant_address):
        self.api_url = api_url
        self.headers = headers
        self.restaurant_external_id = restaurant_external_id
        self.restaurant_name = restaurant_name
        self.restaurant_address = restaurant_address
    
    def call_api(self):
        response = requests.get(self.api_url, headers=self.headers)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"API call failed with status code: {response.status_code}. Response: {response.text}")

    def process_api_response(self, response, table_load_info):
        output = []

        for item in response:
            item_base = {k: v for k, v in item.items() if not isinstance(v, (dict, list))}
            
            # Rename GUID column
            rename_key(item_base, "guid", table_load_info["guid_column"])

            # Extract foreign keys if specified
            if "extract_foreign_keys" in table_load_info:
                for key, new_key in table_load_info["extract_foreign_keys"].items():
                    item_base[new_key] = (item.get(key) if key in item and item.get(key) else {}).get("guid")

            # Append Restaurant Info
            item_base["restaurant_external_id"] = self.restaurant_external_id
            item_base["restaurant_name"] = self.restaurant_name
            item_base["restaurant_address"] = self.restaurant_address

            # Convert date fields
            for date_field in table_load_info["date_columns"]:
                if date_field in item_base:
                    item_base[date_field] = to_mysql_datetime(item_base[date_field])

            output.append(item_base)

        return output

    def load_into_mysql(self, mysql_conn_id, processed_data, table_load_info):
        mysql_hook = MySqlHook(mysql_conn_id=mysql_conn_id)
        connection = mysql_hook.get_conn()
        cursor = connection.cursor()
        logging.info(f"Connected to MySQL with Connection: '{connection.__dict__}'.")
        placeholders = ', '.join(['%s'] * len(table_load_info["mysql_column_names"]))
        columns = ', '.join(f'`{header}`' for header in table_load_info["mysql_column_names"])
        insert_query = f"INSERT INTO {table_load_info['mysql_table_name']} ({columns}) VALUES ({placeholders})"
        logging.info(f"Insert Query: {insert_query}")

        # Prepare the data in the order of column_names
        data = [
            tuple(item.get(column_name, None) for column_name in table_load_info["mysql_column_names"])
            for item in processed_data
        ]

        if table_load_info["mysql_table_name"] == "time_entries":
            # List all unique employee_ids from data
            unique_employee_ids = set(item['employee_id'] for item in processed_data if item.get('employee_id'))
            logging.info(f"Unique Employee IDs in Time Entries Data: {unique_employee_ids}")
            
        cursor.executemany(insert_query, data)
        connection.commit()
        logging.info(f"Inserted {cursor.rowcount} rows into table '{table_load_info['mysql_table_name']} '.")

        cursor.close()
        connection.close()
        logging.info("MySQL connection closed.")


class OrdersAPICalllerDBLoader(APICalllerDBLoader):

    def __init__(self, api_url, headers, restaurant_external_id, restaurant_name, restaurant_address):
        super().__init__(api_url, headers, restaurant_external_id, restaurant_name, restaurant_address)

    def call_api(self):
        response_aggregated = []
        page = 1
        while True:
            paginated_url = f"{self.api_url}&page={page}"
            logging.info(f"Making paginated API call to: '{paginated_url}'")
            response = requests.get(paginated_url, headers=self.headers)
            # logging.info(f"response: {response}")
            logging.info(f"API Response Page {page} contains {len(response.json())} records")
            response_aggregated += response.json()
            # logging.info(f"response_aggregated: {response_aggregated}")
            logging.info(f"Aggregated Response contains {len(response_aggregated)} records")
            if len(response.json()) == 0:  # Stop if the response is an empty array
                logging.info("No more data to fetch. Pagination complete.")
                break
            page += 1
        return response_aggregated
        
    def process_api_response(self, response, table_load_info):
        logging.info(f"Processing Response: {response}")
        orders = super().process_api_response(response, table_load_info["orders"])
        order_checks = []
        order_check_selections = []

        for order in response:
            # Extract order_id for foreign key relationships
            order_id = order.get("guid")

            for check in order.get("checks", []) or []:
                # Remove all those objects that cannot be loaded directly into MySQL
                check_base = {k: v for k, v in check.items() if not isinstance(v, (dict, list))}
                # Rename GUID column
                rename_key(check_base, "guid", "check_id")
                # Extract check_id and order_id for foreign key relationships
                check_id = check_base.get("check_id")
                # add foreign key relationships
                check_base.setdefault("order_id", order_id)
                # convert date fields in check
                for date_field in table_load_info["order_checks"]["date_columns"]:
                    if date_field in check_base:
                        check_base[date_field] = to_mysql_datetime(check_base[date_field])
                # append to order_checks list
                order_checks.append(check_base)

                for sel in check.get("selections", []) or []:
                    # Remove all those objects that cannot be loaded directly into MySQL
                    selection_base = {k: v for k, v in sel.items() if not isinstance(v, (dict, list))}
                    # Rename GUID column
                    rename_key(selection_base, "guid", "selection_id")
                    # Extract foreign keys if specified
                    for key, new_key in table_load_info["order_check_selections"]["extract_foreign_keys"].items():
                        selection_base[new_key] = (sel.get(key) if key in sel and sel.get(key) else {}).get("guid")
                    # add foreign key relationships
                    selection_base.setdefault("order_id", order_id)
                    selection_base.setdefault("check_id", check_id)
                    # convert date fields in selection
                    for date_field in table_load_info["order_check_selections"]["date_columns"]:
                        if date_field in selection_base:    
                            selection_base[date_field] = to_mysql_datetime(selection_base[date_field])
                    # append to order_check_selections list
                    order_check_selections.append(selection_base)

        return orders, order_checks, order_check_selections

    def _log_min_max_created_dates(self, data_name, data):
        created_dates = [record['createdDate'] for record in data if record.get('createdDate')]
        if created_dates:
            logging.info(f"Earliest Created Date for {data_name}: {min(created_dates)}")
            logging.info(f"Latest Created Date for {data_name}: {max(created_dates)}")

    def load_into_mysql(self, mysql_conn_id, processed_data, table_load_info):
        orders, order_checks, order_check_selections = processed_data
        self._log_min_max_created_dates("Orders", orders)
        self._log_min_max_created_dates("Order Checks", order_checks)
        self._log_min_max_created_dates("Order Check Selections", order_check_selections)
        super().load_into_mysql(mysql_conn_id, orders, table_load_info["orders"])
        super().load_into_mysql(mysql_conn_id, order_checks, table_load_info["order_checks"])
        super().load_into_mysql(mysql_conn_id, order_check_selections, table_load_info["order_check_selections"])


# class EmployeesAPICalllerDBLoader(APICalllerDBLoader):

#     def __init__(self, api_url, headers, restaurant_external_id, restaurant_name, restaurant_address):
#         super().__init__(api_url, headers, restaurant_external_id, restaurant_name, restaurant_address)

#     def call_api(self):
#         super().call_api()

#     def process_api_response(self, response, table_load_info):
#         employee = super().process_api_response(response, table_load_info["employee"])

#         employee_job_ref = []
#         for emp in response:
#             if 'jobReferences' in emp and emp['jobReferences']:
#                 for job in emp['jobReferences']:
#                     emp_job_ref = {
#                         "employee_id": emp.get("guid"),
#                         "job_id": job.get("guid"),
#                         "external_id": job.get("externalId")
#                     }
#                     employee_job_ref.append(emp_job_ref)    

#         return employee, employee_job_ref

#     def load_into_mysql(self, mysql_conn_id, processed_data, table_load_info):
#         employee_data, employee_job_ref_data = processed_data
#         super().load_into_mysql(mysql_conn_id, employee_data, table_load_info["employee"])
#         super().load_into_mysql(mysql_conn_id, employee_job_ref_data, table_load_info["employee_job_ref"])

