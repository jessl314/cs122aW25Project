import mysql.connector
import importdata as i


def add_genre(uid, genre):
    """
    add genre to genres string of the user with uid in the users table
    python3 project.py addGenre [uid:int] [genre:str]
    EXAMPLE: python3 project.py addGenre 1 Comedy
    """
    if genre is None or genre == "NULL" or genre == "":
        print("Fail")
        return False

    if uid == 'NULL':
        print("Fail")
        return False
    
    connection = i.create_connection()
    if not connection:
        return False
    cursor = connection.cursor()

    select_query = "SELECT genres FROM users WHERE uid = %s"
    cursor.execute(select_query, (uid,))
    result = cursor.fetchone()

    if result is None:
        print("Fail")
        cursor.close()
        connection.close()
        return False

    current_genres = result[0]

    if current_genres:
        genre_list = current_genres.split(';')
        if genre in genre_list:
            updated_genres = current_genres
            print("Fail")
            return False
        if genre is not None:
            updated_genres = current_genres + ";" + genre
        else:
            updated_genres = current_genres
    else:
        updated_genres = genre if genre is not None else None

    update_query = "UPDATE users SET genres = %s  WHERE uid = %s"

    try:
        cursor.execute(update_query, (updated_genres, uid))
        connection.commit()
        print("Success")
        return True
    except mysql.connector.Error:
        print("Fail")
        return False
    finally:
        cursor.close()
        connection.close()


def delete_viewer(uid):
    """
    Given a Viewer uid, delete the Viewer from the appropriate table(s). Input:
    python3 project.py deleteViewer [uid:int]
    EXAMPLE: python3 project.py deleteViewer 1
    WORKS
    """
    if uid == 'NULL':
        print("Fail")
        return False
    
    connection = i.create_connection()
    if not connection:
        print("Fail")
        return False
    cursor = connection.cursor()

    select_query = "SELECT * FROM viewers WHERE uid = %s"
    cursor.execute(select_query, (uid,))
    result = cursor.fetchone()

    if result is None:
        print("Fail")
        cursor.close()
        connection.close()
        return False
    
    delete_query = "DELETE FROM viewers WHERE uid = %s"

    values = (uid,)
    try:
        cursor.execute(delete_query, values)
        if cursor.rowcount == 0:
            print("Fail")
            return False
        connection.commit()
        print("Success")
        return True
    except mysql.connector.Error:
        print("Fail")
        return False
    finally:
        cursor.close()
        connection.close()



def insert_movie(rid, website_url):
    """
    python3 project.py insertMovie [rid:int] [website_url:str]

    EXAMPLE: python3 project.py insertMovie 1 top-gun.com
    """
    if website_url == 'NULL':
        website_url = None
    if rid == 'NULL':
        print("Fail")
        return False
    
    connection = i.create_connection()
    if not connection:
        print("Fail")
        return False
    cursor = connection.cursor()

    check_query = "SELECT COUNT(*) FROM releases WHERE rid = %s"
    cursor.execute(check_query, (rid,))
    result = cursor.fetchone()

    if result[0] == 0:
        print("Fail")
        return False
    
    insert_query = """
    INSERT INTO movies (rid, website_url)
    VALUES (%s, %s)
    ON DUPLICATE KEY UPDATE website_url = VALUES(website_url);
    """
    try:
        cursor.execute(insert_query, (rid, website_url))
        connection.commit()
        print("Success")
        return True
    except mysql.connector.Error:
        print("Fail")
        return False
    finally:
        cursor.close()
        connection.close()
