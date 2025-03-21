import mysql.connector
import importdata as i  

def insert_viewer(uid, email, nickname, street, city, state, zip_code, genres, joined_date, first, last, subscription):
    """
    Input:
        python3 project.py insertViewer [uid:int] [email:str] [nickname:str] [street:str] [city:str] [state:str] [zip:str] [genres:str] [joined_date:date] [first:str] [last:str] [subscription:str]
        EXAMPLE: python3 project.py insertViewer 1 test@uci.edu awong "1111 1st street" Irvine CA 92616 "romance;comedy" 2020-04-19 Alice Wong yearly
        
    Output:
	    Boolean
    """
    if email == 'NULL':
        email = None
    if nickname == 'NULL':
        nickname = None
    if street == 'NULL':
        street = None
    if city == 'NULL':
        city = None
    if state == 'NULL':
        state = None
    if zip_code == 'NULL':
        zip_code = None
    if genres == 'NULL':
        genres = None
    if joined_date == 'NULL':
        joined_date = None
    if first == 'NULL':
        first = None
    if last == 'NULL':
        last = None
    if subscription == 'NULL':
        subscription = None
    if uid == 'NULL':
        print("Fail")
        return False
    connection = i.create_connection()
    if not connection:
        return False
    cursor = connection.cursor()
    try:
        cursor.execute("INSERT INTO users (uid, email, joined_date, nickname, street, city, state, zip, genres) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                       (uid, email, joined_date, nickname, street, city, state, zip_code, genres))
        cursor.execute("INSERT INTO viewers (uid, subscription, first_name, last_name) VALUES (%s, %s, %s, %s)",
                       (uid, subscription, first, last))
        connection.commit()
        print("Success")
        return True
    except mysql.connector.Error as err:
        print("Fail")        
        return False
    finally:
        cursor.close()
        connection.close()

def insert_session(sid, uid, rid, ep_num, initiate_at, leave_at, quality, device):
    """
    Input:
        python3 project.py insertSession [sid:int] [uid:int] [rid:int] [ep_num:int] [initiate_at:datetime] [leave_at:datetime] [quality:str] [device:str] 
        EXAMPLE: python3 project.py insertSession 1 2 102 4 “2025-01-10 13:10:10” “2025-01-10 15:02:45” 720p mobile
    Output:
	    Boolean
    """
    if initiate_at == 'NULL':
        initiate_at = None
    if leave_at == 'NULL':
        leave_at = None
    if website_url == 'NULL':
        website_url = None
    if quality == 'NULL':
        quality = None
    if device == 'NULL':
        device = None
    if sid == 'NULL' or uid == "NULL" or rid == "NULL" or ep_num == "NULL":
        print("Fail")
        return False
    connection = i.create_connection()
    if not connection:
        return False
    cursor = connection.cursor()
    try:
        cursor.execute("INSERT INTO sessions (sid, uid, rid, ep_num, initiate_at, leave_at, quality, device) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                       (sid, uid, rid, ep_num, initiate_at, leave_at, quality, device))
        connection.commit()
        print("Success")
        return True
    except mysql.connector.Error as err:
        print("Fail")        
        return False
    finally:
        cursor.close()
        connection.close()

def update_release(rid, title):
    """
    Input:
        python3 project.py updateRelease [rid:int] [title:str]
        EXAMPLE: python3 project.py updateRelease 1287 "The Sopranos"
    Output:
	    Boolean
    """
    connection = i.create_connection()
    if not connection:
        return False
    cursor = connection.cursor()
    try:
        cursor.execute("UPDATE releases SET title = %s WHERE rid = %s", (title, rid))
        if cursor.rowcount == 0:
            print("Fail")
            return False
        connection.commit()
        print("Success")
        return True
    except mysql.connector.Error as err:
        print("Fail")
        return False
    finally:
        cursor.close()
        connection.close()

def list_releases(uid):
    """
    Input:
	    python3 project.py listReleases [uid:int]
        EXAMPLE: python project.py listReleases 1
    Output:
	    Table - rid, genre, title
    """
    connection = i.create_connection()
    if not connection:
        return False
    cursor = connection.cursor()
    try:
        cursor.execute("""
            SELECT DISTINCT r.rid, r.genre, r.title
            FROM reviews rv
            JOIN releases r ON rv.rid = r.rid
            WHERE rv.uid = %s
            ORDER BY r.title ASC
        """, (uid,))
        results = cursor.fetchall()
        if results:
            for row in results:
                print(",".join(str(col) for col in row))
            return True
        else:
            print("No records found.")
            return False
    except mysql.connector.Error as err:
        print("Fail")
        return False
    finally:
        cursor.close()
        connection.close()