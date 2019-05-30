import mysql.connector as mariadb

def conf_loader_sql(ip, user, password, database):
    connection = mariadb.connect(host=ip, user=user, password=password, database=database)
    cursor = connection.cursor()
    query = "select Name from windowsServers;"
    cursor.execute(query)
    result = cursor.fetchall()
    connection.close()
    return result
