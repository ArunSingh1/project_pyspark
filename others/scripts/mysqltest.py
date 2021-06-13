import mysql.connector

cnx = mysql.connector.connect(user='pyspark', password='password',
                              host='localhost',
                              database='twitterdb', auth_plugin='mysql_native_password')
cursor = cnx.cursor()


#query = """CREATE table IF NOT EXISTS dude (task_id INT AUTO_INCREMENT PRIMARY KEY,username VARCHAR(255) NOT NULL,created_at DATE,retweet_count INT NOT NULL,tweet TEXT)"""
query = """CREATE TABLE IF NOT EXISTS dudeee (
    username VARCHAR(255) ,
    created_at VARCHAR(255),
    retweet_count VARCHAR(255),
    tweet TEXT
)"""
cursor.execute(query)



# cursor.execute("query")
cursor.close()
cnx.close()