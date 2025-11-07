from nc_concepts_toast_rest_extract_dly import airflow_common
from nc_concepts_toast_rest_extract_dly import api_caller_db_loader

TOAST_CONN_ID = 'toast_credentials'
MYSQL_INGEST_DB_CONN_ID = 'nc_concepts_mysql'
MYSQL_PROCESSED_DB_CONN_ID = 'nc_concepts_processed_mysql'
MYSQL_BUSAN_MART_ONLY_DB_CONN_ID = 'nc_concepts_busam_mart_only_mysql'


CONFIG_RESTAURANT_LIST = [
    {
        "external_id": "45616d1b-f2a6-42bd-88cc-3e0388102d1c",
        "name_simple": "busan_mart_culdesac",
        "name": "Busan Mart - Culdesac",
        "address": "2005 E Apache Blvd Suite 101, Tempe, AZ 85281"
    },
    {
        "external_id": "63053c0d-18cc-4e72-96d5-df38254cde05",
        "name_simple": "busan_mart_tempe",
        "name": "Busan Mart - Tempe",
        "address": "1310 E Broadway Rd #101, Tempe, AZ 85282"
    },
    {
        "external_id": "fc3afbed-33e1-42e2-9ebe-68cd3d1fa242",
        "name_simple": "busan_mart_2",
        "name": "Busan Mart - 2",
        "address": "8001 E Roosevelt St Scottsdale, AZ 85257"
    },
    {
        "external_id": "c7ce3778-adab-4d66-85c2-63b7f8d4fc0b",
        "name_simple": "bei_express",
        "name": "Bei Express",
        "address": "2910 Hayden Rd, Scottsdale, AZ 85251"
    },
    {
        "external_id": "3428f2ad-75a2-4c3f-abfd-1959a0a6da50",
        "name_simple": "zu_izakaya",
        "name": "Zu Izakaya Sushi Bar (ASU)",
        "address": "1212 E Apache Blvd"
    },
    {
        "external_id": "08049c83-a37f-4e20-830d-1256d25d8956",
        "name_simple": "pocha_54",
        "name": "Pocha 54 Korean Tapas",
        "address": "5415 E High St, Ste 101"
    }
]

CONFIG_INGEST_TASKS = [
    {
        "task_id": "get_bearer_token",
        "task_type": "bearer_token",
        "python_callable": airflow_common.get_toast_bearer_token
    },
    ## Skipping Employees for now
    # {
    #     "task_id": "get_and_load_employees",
    #     "task_type": "get_and_load",
    #     "api_url": "https://ws-api.toasttab.com/labor/v1/employees/",
    #     "python_callable": airflow_common.get_and_load_toast_data,
    #     "table_load_info": {
    #         # MySQL Table: employee
    #         "employee": {
    #             "guid_column": "employee_id",
    #             "date_columns": ["createdDate", "deletedDate", "modifiedDate"],
    #             "mysql_table_name": "employee",
    #             "mysql_column_names": ["employee_id", "entityType", "externalId", "v2EmployeeGuid", "lastName", "phoneNumberCountryCode", "firstName", "chosenName", "createdDate", "phoneNumber", "deleted", "deletedDate", "modifiedDate", "externalEmployeeId", "email", "restaurant_external_id", "restaurant_name", "restaurant_address"]
    #         },
    #         # MySQL Table: employee_job_ref
    #         "employee_job_ref": {
    #             "guid_column": "job_id",
    #             "date_columns": [],
    #             "mysql_table_name": "employee_job_ref",
    #             "mysql_column_names": ["employee_id", "job_id", "external_id"]
    #         }
    #     },
    #     "api_caller_db_loader_class": api_caller_db_loader.EmployeesAPICalllerDBLoader,
    #     "depends_on": ["get_bearer_token"]
    # },
    {
        "task_id": "get_and_load_time_entries",
        "task_type": "get_and_load",
        "api_url": "https://ws-api.toasttab.com/labor/v1/timeEntries?startDate={{ execution_date.strftime('%Y-%m-%d') }}T00%3A00%3A00.000%2B0400&endDate={{ execution_date.strftime('%Y-%m-%d') }}T23%3A59%3A59.000%2B0400",
        "python_callable": airflow_common.get_and_load_toast_data,
        "table_load_info": {
            "guid_column": "time_entry_id",
            "date_columns": ["outDate", "inDate", "createdDate", "deletedDate", "modifiedDate"],
            "extract_foreign_keys": {
                "employeeReference": "employee_id",
                "jobReference": "job_id"
            },
            "mysql_table_name": "time_entries",
            "mysql_column_names": ["entityType", "externalId", "nonCashSales", "outDate", "overtimeHours", "breaks", "shiftReference", "nonCashGratuityServiceCharges", "inDate", "regularHours", "tipsWithheld", "businessDate", "cashGratuityServiceCharges", "createdDate", "deleted", "deletedDate", "cashSales", "hourlyWage", "nonCashTips", "modifiedDate", "declaredCashTips", "autoClockedOut", "time_entry_id", "employee_id", "job_id", "restaurant_external_id", "restaurant_name", "restaurant_address"]
        },
        "api_caller_db_loader_class": api_caller_db_loader.APICalllerDBLoader,
        "depends_on": ["get_bearer_token"]
    },
    {
        "task_id": "get_and_load_orders",
        "task_type": "get_and_load",
        "api_url": "https://ws-api.toasttab.com/orders/v2/ordersBulk?startDate={{ execution_date.strftime('%Y-%m-%d') }}T00%3A00%3A00.000%2B0400&endDate={{ execution_date.strftime('%Y-%m-%d') }}T23%3A59%3A59.000%2B0400",
        "python_callable": airflow_common.get_and_load_toast_data,
        "table_load_info": {
            # MySQL Table: orders
            "orders": {
                "guid_column": "order_id",
                "date_columns": ["voidDate", "paidDate", "estimatedFulfillmentDate", "openedDate", "deletedDate", "modifiedDate", "promisedDate", "createdDate", "closedDate"],
                "extract_foreign_keys": {"server": "server_id"},
                "mysql_table_name": "orders",
                "mysql_column_names": ["entityType", "externalId", "revenueCenter", "displayNumber", "source", "voidDate", "paidDate", "voided", "estimatedFulfillmentDate", "requiredPrepTime", "openedDate", "deletedDate", "modifiedDate", "promisedDate", "createdInTestMode", "duration", "businessDate", "excessFood", "table", "approvalStatus", "deliveryInfo", "serviceArea", "curbsidePickupInfo", "numberOfGuests", "diningOption", "appliedPackagingInfo", "voidBusinessDate", "deleted", "createdDate", "closedDate", "channelGuid", "order_id", "server_id", "restaurant_external_id", "restaurant_name", "restaurant_address"]
            },
            # MySQL Table: order_checks
            "order_checks": {
                "guid_column": "check_id",
                "date_columns": ["voidDate", "paidDate", "openedDate", "createdDate", "deletedDate", "modifiedDate", "closedDate"],
                "mysql_table_name": "order_checks",
                "mysql_column_names": ["entityType", "externalId", "displayNumber", "voidDate", "duration", "openedBy", "paidDate", "appliedLoyaltyInfo", "voided", "paymentStatus", "amount", "tabName", "taxExempt", "taxExemptionAccount", "openedDate", "totalAmount", "voidBusinessDate", "createdDate", "deleted", "closedDate", "deletedDate", "modifiedDate", "taxAmount", "customer", "check_id", "order_id"]
            },
            # MySQL Table: order_check_selections
            "order_check_selections": {
                "guid_column": "selection_id",
                "date_columns": ["voidDate", "modifiedDate", "createdDate"],
                "extract_foreign_keys": {"salesCategory": "salesCategoryId", "itemGroup": "itemGroupId", "item": "itemId", "diningOption": "diningOptionId"},
                "mysql_table_name": "order_check_selections",
                "mysql_column_names": ["entityType", "externalId", "voidReason", "voidDate", "fulfillmentStatus", "optionGroupPricingMode", "salesCategory", "selectionType", "price", "voided", "storedValueTransactionId", "unitOfMeasure", "refundDetails", "toastGiftCard", "tax", "modifiedDate", "deferred", "preDiscountPrice", "optionGroup", "displayName", "seatNumber", "giftCardSelectionInfo", "splitOrigin", "taxInclusion", "quantity", "receiptLinePrice", "diningOption", "voidBusinessDate", "createdDate", "preModifier", "fulfillment", "selection_id", "salesCategoryId", "itemGroupId", "itemId", "diningOptionId", "order_id", "check_id"]
            }
        },
        "api_caller_db_loader_class": api_caller_db_loader.OrdersAPICalllerDBLoader,
        "depends_on": ["get_bearer_token"]
    }
]

#todo: set dependencies between processed tasks
CONFIG_PROCESSED_TASKS = [
    {
        "task_id": f"load_staff_time_entry",
        "conn_id": MYSQL_PROCESSED_DB_CONN_ID,
        "sql_file": "sql/staff_time_entry.sql"
    },
    {
        "task_id": f"load_staff_shift_summarized",
        "conn_id": MYSQL_PROCESSED_DB_CONN_ID,
        "sql_file": "sql/staff_shift_summarized.sql",
        "depends_on": [f"load_staff_time_entry"]
    },
    {
        "task_id": f"load_tickets",
        "conn_id": MYSQL_PROCESSED_DB_CONN_ID,
        "sql_file": "sql/tickets.sql"
    },
    {
        "task_id": f"load_ticket_items",
        "conn_id": MYSQL_PROCESSED_DB_CONN_ID,
        "sql_file": "sql/ticket_items.sql"
    },
    {
        "task_id": f"load_tickets_summarized",
        "conn_id": MYSQL_PROCESSED_DB_CONN_ID,
        "sql_file": "sql/tickets_summarized.sql",
        "depends_on": ["load_tickets", "load_ticket_items"]
    },
    {
        "task_id": f"load_busan_mart_only_staff_time_entry",
        "conn_id": MYSQL_BUSAN_MART_ONLY_DB_CONN_ID,
        "sql_file": "sql/busan_mart_only_staff_time_entry.sql",
        "depends_on": [f"load_staff_time_entry"]
    },
    {
        "task_id": f"load_busan_mart_only_staff_shift_summarized",
        "conn_id": MYSQL_BUSAN_MART_ONLY_DB_CONN_ID,
        "sql_file": "sql/busan_mart_only_staff_shift_summarized.sql",
        "depends_on": [f"load_staff_shift_summarized"]
    },
    {
        "task_id": f"load_busan_mart_only_tickets",
        "conn_id": MYSQL_BUSAN_MART_ONLY_DB_CONN_ID,
        "sql_file": "sql/busan_mart_only_tickets.sql",
        "depends_on": [f"load_tickets"]
    },
    {
        "task_id": f"load_busan_mart_only_ticket_items",
        "conn_id": MYSQL_BUSAN_MART_ONLY_DB_CONN_ID,
        "sql_file": "sql/busan_mart_only_ticket_items.sql",
        "depends_on": ["load_ticket_items"]
    },
    {
        "task_id": f"load_busan_mart_only_tickets_summarized",
        "conn_id": MYSQL_BUSAN_MART_ONLY_DB_CONN_ID,
        "sql_file": "sql/busan_mart_only_tickets_summarized.sql",
        "depends_on": ["load_tickets_summarized"]
    }
]

