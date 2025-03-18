import sys
import importdata as i
import task345 as t
import last_tasks as b


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
        except ValueError as e:
            print("Fail")
            return False
    elif command == "videosViewed" and len(sys.argv) == 3:
        #python project.py videosViewed 123
        b.video_viewed(sys.argv[2])

if __name__ == "__main__":
    main()
