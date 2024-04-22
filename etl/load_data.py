import pandas
import pandas_gbq
def load_data_to_bigquery(df, table_id, project_id):
    table_id = project_id
    project_id = project_id
    pandas_gbq.togbq(df, table_id, project_id)