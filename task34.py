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
    """
    connection = i.create_connection()
    if not connection:
        return False
    cursor = connection.cursor()
    delete_query = "DELETE FROM viewers V WHERE V.uid = %s "

    values = (uid)
    try:
        cursor.execute(delete_query, values)
        connection.commit()
        print(f"viewer with uid {uid} deleted successfully from the viewers table")
        return True
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return False
    finally:
        cursor.close()
        connection.close()

