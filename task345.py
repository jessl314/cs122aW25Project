import mysql.connector
import importdata as i


def add_genre(uid, genre):
    """
    add genre to genres string of the user with uid in the users table
    python3 project.py addGenre [uid:int] [genre:str]
    EXAMPLE: python3 project.py addGenre 1 Comedy

    WORKS: for appending a genre at least LOL
    CHECK: adding a genre when genres is null
    """
    connection = i.create_connection()
    if not connection:
        return False
    cursor = connection.cursor()
    update_query = "UPDATE Users SET genres = IFNULL(CONCAT(genres, ';', %s), %s) WHERE uid = %s"

    values = (genre, genre, uid)

    try:
        cursor.execute(update_query, values)
        connection.commit()
        print(f"genre {genre} updated successfully for uid {uid}")
        return True
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return False
    finally:
        cursor.close()
        connection.close()

# delete viewer : DELETE FROM viewer V WHERE V.uid = {uid}

def delete_viewer(uid):
    """
    Given a Viewer uid, delete the Viewer from the appropriate table(s). Input:
    python3 project.py deleteViewer [uid:int]
    EXAMPLE: python3 project.py deleteViewer 1
    WORKS
    """
    connection = i.create_connection()
    if not connection:
        return False
    cursor = connection.cursor()
    delete_query = "DELETE FROM viewers WHERE uid = %s"

    values = (uid,)
    try:
        cursor.execute(delete_query, values)
        if cursor.rowcount == 0:
            print(f"No viewer found with uid {uid}. No rows were deleted.")
            return False
        connection.commit()
        print(f"viewer with uid {uid} deleted successfully from the viewers table")
        return True
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return False
    finally:
        cursor.close()
        connection.close()



def insert_movie(rid, website_url):
    """
    python3 project.py insertMovie [rid:int] [website_url:str]

    EXAMPLE: python3 project.py insertMovie 1 top-gun.com
    """
    connection = i.create_connection()
    if not connection:
        return False
    cursor = connection.cursor()
    insert_query = """
    INSERT INTO movies (rid, website_url)
    SELECT %s, %s
    FROM releases r
    WHERE r.rid = %s 
    ON DUPLICATE KEY UPDATE website_url = VALUES(website_url)"""
    values = (rid, website_url, rid)
    try:
        cursor.execute(insert_query, values)
        if cursor.rowcount == 0:
            print(f"corresponding rid {rid} not found in release. No rows were deleted.")
            return False
        connection.commit()
        print(f"movie url {website_url} successfully added for rid {rid}")
        return True
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return False
    finally:
        cursor.close()
        connection.close()
