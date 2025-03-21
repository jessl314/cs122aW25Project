import mysql.connector
import importdata as i  

def insert_viewer(uid, email, nickname, street, city, state, zip_code, genres, joined_date, first, last, subscription):
    """
    Input:
        python3 project.py insertViewer [uid:int] [email:str] [nickname:str] [street:str] [city:str] [state:str] [zip:str] [genres:str] [joined_date:date] [first:str] [last:str] [subscription:str]
        EXAMPLE: python project.py insertViewer 1 test@uci.edu awong "1111 1st street" Irvine CA 92616 "romance;comedy" 2020-04-19 Alice Wong yearly
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
    else:
        valid = {"free", "monthly", "yearly"}  
        if subscription and subscription not in valid:
            print("Fail")
            return False
    if uid == 'NULL':
        print("Fail")
        return False
    
    connection = i.create_connection()
    if not connection:
        print("Fail")
        return False
    cursor = connection.cursor()


    try:
        cursor.execute("START TRANSACTION")
        
        cursor.execute("SELECT COUNT(*) FROM viewers WHERE uid = %s", (uid,))
        viewer_exists = cursor.fetchone()[0] > 0  # Check if viewer exists

        cursor.execute("SELECT COUNT(*) FROM users WHERE uid = %s", (uid,))
        user_exists = cursor.fetchone()[0] > 0  # Check if user exists

        if viewer_exists and not user_exists:  # If the viewer exists but not in users, fail
            print("Fail")
            connection.rollback()
            return False
        
        cursor.execute("""
            INSERT INTO users (uid, email, joined_date, nickname, street, city, state, zip, genres) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                email = VALUES(email), 
                joined_date = VALUES(joined_date), 
                nickname = VALUES(nickname), 
                street = VALUES(street), 
                city = VALUES(city), 
                state = VALUES(state), 
                zip = VALUES(zip), 
                genres = VALUES(genres);
        """, (uid, email, joined_date, nickname, street, city, state, zip_code, genres))

        cursor.execute("""
            INSERT INTO viewers (uid, subscription, first_name, last_name) 
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                subscription = VALUES(subscription), 
                first_name = VALUES(first_name), 
                last_name = VALUES(last_name);
        """, (uid, subscription, first, last))
        
        connection.commit()
        print ("Success")
        return True
    except mysql.connector.Error as err:
        connection.rollback()
        print("Fail")        
        return False
    finally:
        cursor.close()
        connection.close()

def insert_session(sid, uid, rid, ep_num, initiate_at, leave_at, quality, device):
    """
    Input:
        python3 project.py insertSession [sid:int] [uid:int] [rid:int] [ep_num:int] [initiate_at:datetime] [leave_at:datetime] [quality:str] [device:str] 
        EXAMPLE: python project.py insertSession 1 2 102 4 "2025-01-10 13:10:10" "2025-01-10 15:02:45" 720p mobile
    Output:
        Boolean
    """
    if sid == 'NULL' or uid == "NULL" or rid == "NULL" or ep_num == "NULL":
        print("Fail")
        return False
    
    
    # Handle NULL values
    if initiate_at == 'NULL':
        initiate_at = None
    if leave_at == 'NULL':
        leave_at = None
    if quality == 'NULL':
        quality = None
    else:
        # Validate quality values
        valid_quality = {"480p", "720p", "1080p"}
        if quality and quality not in valid_quality:
            print("Fail")
            return False
            
    if device == 'NULL':
        device = None
    else:
        # Validate device values
        valid_device = {"mobile", "desktop"}
        if device and device not in valid_device:
            print("Fail")
            return False
    
    connection = i.create_connection()
    if not connection:
        print("Fail")
        return False
    cursor = connection.cursor()
    
    try:
        # Check if the referenced viewer exists
        cursor.execute("SELECT uid FROM viewers WHERE uid = %s", (uid,))
        if not cursor.fetchone():
            print("Fail")
            return False
            
        # Check if the referenced video exists
        cursor.execute("SELECT rid, ep_num FROM videos WHERE rid = %s AND ep_num = %s", (rid, ep_num))
        if not cursor.fetchone():
            print("Fail")
            return False
        
        # Insert the session
        cursor.execute("""
            INSERT INTO sessions (sid, uid, rid, ep_num, initiate_at, leave_at, quality, device) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (sid, uid, rid, ep_num, initiate_at, leave_at, quality, device))
        
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
    #print("Hello")
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
            #print("No records found.")
            #print("Fail")
            return False
    except mysql.connector.Error as err:
        #print("Fail")
        return False
    finally:
        cursor.close()
        connection.close()