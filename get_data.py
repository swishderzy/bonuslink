import pandas as pd
import mysql.connector
import re




class data_extractor(object):

    """
    get data from speedrent database
    """
            
    def sqlToCsv(q = r"select * from property limit 1;", table = "property", extract_cols = False, database = "speedrent"):
        if extract_cols:
            assert table in q, "tables must be the same"

        conn = mysql.connector.Connect(host='127.0.0.1', port = "3307", user='khaled', password='9FXFto7OUObdI9Qf', database=database,auth_plugin='mysql_native_password')
        cursor = conn.cursor()
        cursor.execute(q)
        df = pd.DataFrame(cursor.fetchall())

        if extract_cols:
            conn = mysql.connector.Connect(host='127.0.0.1', port = "3307", user='khaled', password='9FXFto7OUObdI9Qf', database=database,auth_plugin='mysql_native_password')
            cursor = conn.cursor()
            cursor.execute(f"SHOW columns FROM {table}")
            cols = [i[0] for i in cursor.fetchall()]
            df.columns = cols
        else:
            try:
                cols = re.findall("select (.*) from", q.lower())[0].split(", ")
            except:
                cols = re.findall("select (.*) from", q.lower())[0].split(",")
            df.columns = cols
        cursor.close()
        conn.close()
        return df


    def SQL_Query(query, database = "speedrent"):
        conn = mysql.connector.Connect(host='127.0.0.1', port = "3307", user='khaled', password='9FXFto7OUObdI9Qf', database=database,auth_plugin='mysql_native_password')
        cursor = conn.cursor()
        cursor.execute(query)
        r = cursor.fetchall()
        cursor.close()
        conn.close()
        return r


