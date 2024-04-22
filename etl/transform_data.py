import pandas as pd

#STAGE1: transform data from NoSQL document to dataframe
def transform_combination(data,columns_list, event_timestamp_name):
    tmp_list = []
    for i in range(len(data)):
        for column in columns_list:
            tmp_list.append(data[i]["event_data"][column])
        tmp_list.append(data[i]["event_timestamp"])
    df = pd.DataFrame(tmp_list, columns = columns_list.append(event_timestamp_name)) 
    return df

def transform_user_registered(data):
    tmp_list = []
    for i in range(len(data)):
        tmp_list.append([data[i]["event_data"]["user_id"]], [data[i]["event_data"]["user_email"]], [data[i]["event_timestamp"]])

    df = pd.DataFrame(tmp_list, columns = ["user_id", "user_email", "event_timestamp"])
    return df

def transform_product_viewed(data):
    tmp_list = []
    for i in range(len(data)):
        tmp_list.append([data[i]["event_data"]["user_id"]],[data[i]["event_data"]["product_id"]],[data[i]["event_data"]["product_name"]], [data[i]["event_timestamp"]])

    df = pd.DataFrame(tmp_list, columns = ["user_id", "product_id", "product_name", "event_timestamp"])
    return df

def transform_cart_updated(data):
    tmp_list = []
    for i in range(len(data)):
        tmp_list.append([data[i]["event_data"]["user_id"]],[data[i]["event_data"]["product_id"]],[data[i]["event_data"]["quantity"]],[data[i]["event_data"]["price"]], [data[i]["event_timestamp"]])

    df = pd.DataFrame(tmp_list, columns = ["user_id", "product_id", "quantity", "price", "event_timestamp"])
    return df

def transform_payment_completed(data):
    tmp_list = []
    for i in range(len(data)):
        tmp_list.append([data[i]["event_data"]["user_id"]],[data[i]["event_data"]["order_id"]],[data[i]["event_data"]["payment_method"]],[data[i]["event_data"]["total_amount"]], [data[i]["event_timestamp"]])

    df = pd.DataFrame(tmp_list, columns = ["user_id", "order_id", "payment_method", "total_amount", "event_timestamp"])
    return df

def transform_product_purchased(data):
    tmp_list = []
    for i in range(len(data)):
        tmp_list.append([data[i]["event_data"]["user_id"]],[data[i]["event_data"]["order_id"]],[data[i]["event_data"]["product_id"]],[data[i]["event_data"]["product_name"]],[data[i]["event_data"]["quantity"]],[data[i]["event_data"]["price"]], [data[i]["event_timestamp"]])

    df = pd.DataFrame(tmp_list, columns = ["user_id", "order_id", "product_id","product_name", "quantity", "price", "event_timestamp"])
    return df

def transform_order_cancelled(data):
    tmp_list = []
    for i in range(len(data)):
        tmp_list.append([data[i]["event_data"]["user_id"]],[data[i]["event_data"]["order_id"]],[data[i]["event_data"]["cancellation_reason"]], [data[i]["event_timestamp"]])

    df = pd.DataFrame(tmp_list, columns = ["user_id", "order_id", "cancellation_reason", "event_timestamp"])
    return df

#STAGE2: transform data into table that fit business analtsis needs
#第二階段 transform 步驟：將資料整理成商業分析所使用的表
def transform_user_activity_analysis(df_user_registered, df_payment_completed, df_product_view):
    #合併註冊及 payment
    merged_df = pd.merge(df_user_registered, df_payment_completed, how= "outer", left_on='user_id', right_on='user_id')
    #再合併瀏覽紀錄
    merged_df.merge(df_product_view, how= "outer", on = "user_id")
    df = merged_df[["user_id","user_email", "register_timestamp", "view_timestamp", "payment_timestamp"]]
    
    for i in range(len(df.size)):
        if df["view_timestamp"][i] is not None:
            df["event_type"][i] = "product_view"
