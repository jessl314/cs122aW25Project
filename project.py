import sys
import importdata as i
import task345 as t
import task2678 as a
import last_tasks as b

#het hi

def main():
    """
    main function where all commands will be run from
    
    """
    if len(sys.argv) < 2:
        print("Usage: python project.py <command> [args]")
        sys.exit(1)
    command = sys.argv[1]
    if command == 'import' and len(sys.argv) == 3:
        folder_name = sys.argv[2]
        i.import_data(folder_name)
    elif command == "addGenre" and len(sys.argv) == 4:
        uid = sys.argv[2]
        genre = sys.argv[3]
        t.add_genre(uid, genre)
    elif command == "deleteViewer" and len(sys.argv) == 3:
        uid = sys.argv[2]
        t.delete_viewer(uid)
    elif command == "insertMovie" and len(sys.argv) == 4:
        rid = sys.argv[2]
        url = sys.argv[3]
        t.insert_movie(rid, url)
    elif command == "popularRelease" and len(sys.argv) == 3:
        number = sys.argv[2]
        b.top_release(number)
    elif command == "releaseTitle" and len(sys.argv) == 3:
        b.find_release(sys.argv[2])
    elif command == "activeViewer" and len(sys.argv) == 5:
        #python project.py activeViewer 1 2025-01-01 2025-01-20
        try:
            times = int(sys.argv[2])
            b.find_viewers(times, sys.argv[3], sys.argv[4])
        except ValueError:
            print("Fail")
            return False
    elif command == "videosViewed" and len(sys.argv) == 3:
        #python project.py videosViewed 123
        b.video_viewed(sys.argv[2])
    #2
    elif command == "insertViewer" and len(sys.argv) == 14:
        uid = sys.argv[2]
        email = sys.argv[3]
        nickname = sys.argv[4]
        street = sys.argv[5]
        city = sys.argv[6]
        state = sys.argv[7]
        zip_code = sys.argv[8]
        genres = sys.argv[9]
        joined_date = sys.argv[10]
        first = sys.argv[11]
        last = sys.argv[12]
        subscription = sys.argv[13]
        a.insert_viewer(uid, email, nickname, street, city, state, zip_code, genres, joined_date, first, last, subscription)
    #6
    elif command == "insertSession" and len(sys.argv) == 10:
        sid = sys.argv[2]
        uid = sys.argv[3]
        rid = sys.argv[4]
        ep_num = sys.argv[5]
        initiate_at = sys.argv[6]
        leave_at = sys.argv[7]
        quality = sys.argv[8]
        device = sys.argv[9]
        print("yes")
        a.insert_session(sid, uid, rid, ep_num, initiate_at, leave_at, quality, device)
        print("done")
    #7
    elif command == "updateRelease" and len(sys.argv) == 4:
        rid = sys.argv[2]
        title = sys.argv[3]
        a.update_release(rid,title)
    #8
    elif command == "listReleases" and len(sys.argv) == 3:
        uid = sys.argv[2]
        a.list_releases(uid)
    else:
        print(len(sys.argv))

#akl
if __name__ == "__main__":
    main()
