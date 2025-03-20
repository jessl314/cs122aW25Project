import mysql.connector
import importdata as i


def top_release(number):
    """
	List the top N releases that have the most reviews, in DESCENDING order on reviewCount, rid.
    Input:
        python3 project.py popularRelease [N: int]
        EXAMPLE: python project.py popularRelease 10
    Output:
        Table - rid, title, reviewCount
    """
    try:
        number = int(number)
    except ValueError:
        print("Fail")
        return False

    if not isinstance(number, int) or number <= 0:
        print("Fail")
        return False

    connection = i.create_connection()
    if not connection:
        print("Fail")
        return False
    cursor = connection.cursor()
    query = """
        SELECT r.rid, r.title, COUNT(rv.rid) AS reviewCount 
        FROM releases r
        JOIN reviews rv ON r.rid = rv.rid
        GROUP BY r.rid, r.title
        ORDER BY reviewCount DESC, r.rid DESC
        LIMIT %s;
        """

    try:
        cursor.execute(query, (number,))
        results = cursor.fetchall()
        for row in results:
            print(",".join(map(str, row)))
        return True
    except mysql.connector.Error:
        print("Fail")
        return False
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def find_release(sid):
    """
    Title of Release
    Given a session ID, find the release associated with the video streamed in the session.
    List information on both the release and video, in ASCENDING order on release title.
    Input:
        python3 project.py releaseTitle [sid: int]
        EXAMPLE: python project.py releaseTitle 1
    Output:
        Table - rid, release_title, genre, video_title, ep_num, length

    """
    connection = i.create_connection()
    if not connection:
        print("Fail")
        return False
    cursor = connection.cursor()
    query = """
        SELECT r.rid, r.title AS release_title, r.genre, v.title AS video_title, v.ep_num, v.length
        FROM releases r
        JOIN videos v ON r.rid = v.rid
        JOIN sessions s ON s.rid = v.rid AND s.ep_num = v.ep_num
        WHERE s.sid = %s
        ORDER BY r.title ASC;
    """

    values = (sid,)
    try:
        cursor.execute(query, values)
        result = cursor.fetchall()
        if not result:
            print("Fail")
            return False
        for row in result:
            print(",".join(map(str, row)))
        return True
    except mysql.connector.Error:
        print("Fail")
        return False
    finally:
        cursor.close()
        connection.close()



def find_viewers(times, start_date, end_date):
    """
    Find all active viewers that have started a session more than N times (including N)
    in a specific time range (including start and end date), in ASCENDING order by uid.
    N will be at least 1.
    Input:
        python3 project.py activeViewer [N:int] [start:date] [end:date]
        EXAMPLE: python project.py activeViewer 5 2023-01-09 2023-03-10
    Output:
        Table - UID, first name, last name

    """
    connection = i.create_connection()
    if not connection:
        return False
    cursor = connection.cursor()
    insert_query = """
    SELECT v.uid, first_name, last_name
    FROM viewers v
    JOIN sessions s ON s.uid = v.uid
    WHERE s.initiate_at BETWEEN %s AND %s
    GROUP BY v.uid
    HAVING COUNT(v.uid) >= %s
    ORDER BY v.uid ASC"""
    values = (start_date, end_date, times)
    try:
        cursor.execute(insert_query, values)
        result = cursor.fetchall()
        if not result:
            print("Fail")
            return False
        for row in result:
            print(",".join(map(str, row)))
        return True
    except mysql.connector.Error:
        print("Fail")
        return False
    finally:
        cursor.close()
        connection.close()

def video_viewed(rid):
    """
    Given a Video rid, count the number of unique viewers that have started a session on it.
    Videos that are not streamed by any viewer should have a count of 0 instead of NULL.
    Return video information along with the count in DESCENDING order by rid.
    Input:
        python3 project.py videosViewed [rid: int]
    EXAMPLE: python project.py videosViewed 123
    Output:
        Table - RID,ep_num,title,length,COUNT
    """
    connection = i.create_connection()
    if not connection:
        print("Fail")
        return False
    cursor = connection.cursor()
    insert_query = """
        SELECT v.rid ,v.ep_num,v.title,v.length,COALESCE(COUNT(DISTINCT vi.uid), 0) as viewers_count
        FROM videos v
        LEFT JOIN sessions s ON s.rid = v.rid
        LEFT JOIN viewers vi ON vi.uid = s.uid
        WHERE v.rid = %s
        GROUP BY v.rid, v.ep_num, v.title, v.length
        ORDER BY viewers_count DESC;"""
    values = (rid, )
    try:
        cursor.execute(insert_query, values)
        result = cursor.fetchall()
        if not result:
            print("Fail")
            return False
        for row in result:
            print(",".join(map(str, row)))
        return True
    except mysql.connector.Error:
        print("Fail")
        return False
    finally:
        cursor.close()
        connection.close()
