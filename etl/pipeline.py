import extract_data
import transform_data
import load_data

def etl_pipeline_user_register():
    extracted_data = extract_data.extract_document("user_registered")
    columns_list = ["user_id", "user_email"]
    transformed_data = transform_data.transform_combination(extracted_data, columns_list, "user_registered_timestamp")
    load_data.load_data_to_bigquery(transformed_data, 1, 1)  #table_id ,  #project_id


