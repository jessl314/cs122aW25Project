import mysql.connector
import importdata as i

# TODO: replace all print statements with success or failure

def add_genre(uid, genre):
    """
    add genre to genres string of the user with uid in the users table
    python3 project.py addGenre [uid:int] [genre:str]
    EXAMPLE: python3 project.py addGenre 1 Comedy

    WORKS: for appending a genre at least LOL
    CHECK: adding a genre when genres is null
    """
    if genre == "NULL":
        genre = None

    if uid == 'NULL':
        print("❌ Error: rid cannot be NULL.")
        return False
    
    connection = i.create_connection()
    if not connection:
        return False
    cursor = connection.cursor()

    select_query = "SELECT genres FROM users WHERE uid = %s"
    cursor.execute(select_query, (uid,))
    result = cursor.fetchone()

    if result is None:
        print(f"Error: No user found with uid {uid}")
        cursor.close()
        connection.close()
        return False

    current_genres = result[0]

    if current_genres:
        genre_list = current_genres.split(';')
        if genre in genre_list:
            print(f"Genre '{genre}' already exists for user {uid}")
        if genre is not None:
            updated_genres = current_genres + ";" + genre
        else:
            updated_genres = current_genres
    else:
        updated_genres = genre

    update_query = "UPDATE Users SET genres = %s  WHERE uid = %s"

    try:
        cursor.execute(update_query, (updated_genres, uid))
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
    if uid == 'NULL':
        print("❌ Error: uid cannot be NULL.")
        return False
    
    connection = i.create_connection()
    if not connection:
        return False
    cursor = connection.cursor()

    select_query = "SELECT * FROM viewers WHERE uid = %s"
    cursor.execute(select_query, (uid,))
    result = cursor.fetchone()

    if result is None:
        print(f"Error: No user found with uid {uid}")
        cursor.close()
        connection.close()
        return False
    
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
    if website_url == 'NULL':
        website_url = None
    if rid == 'NULL':
        print("❌ Error: rid cannot be NULL.")
        return False
    
    connection = i.create_connection()
    if not connection:
        return False
    cursor = connection.cursor()

    check_query = "SELECT COUNT(*) FROM releases WHERE rid = %s"
    cursor.execute(check_query, (rid,))
    result = cursor.fetchone()

    if result[0] == 0:
        print(f"❌ Error: Release with rid {rid} does not exist.")
        return False
    
    insert_query = """
    INSERT INTO movies (rid, website_url)
    VALUES (%s, %s)
    ON DUPLICATE KEY UPDATE website_url = VALUES(website_url);
    """
    try:
        cursor.execute(insert_query, (rid, website_url))
        connection.commit()
        print(f"movie url {website_url} successfully added for rid {rid}")
        return True
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return False
    finally:
        cursor.close()
        connection.close()
