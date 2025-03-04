# import sys
import mysql.connector
#testing
#test
db = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="root"
)

mycursor = db.cursor()
