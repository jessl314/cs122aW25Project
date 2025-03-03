# import sys
import mysql.connector

db = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="root"
)

mycursor = db.cursor()
