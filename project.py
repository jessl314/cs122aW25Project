import sys
import importdata as i
import task345 as t
import last_tasks as b

#TODO think about the contraints on the command
#TODO If the input is NULL, treat it as the None type in Python, not a string called “NULL”.

def check_success(success, msg1, msg2):
    """prints message based on whether a function succeeded or not"""
    if success:
        print(msg1)
    else:
        print(msg2)

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
        success = i.import_data(folder_name)
        check_success(success, "Data imported successfully.", "Failed to import data.")
    elif command == "addGenre" and len(sys.argv) == 4:
        uid = sys.argv[2]
        genre = sys.argv[3]
        success = t.add_genre(uid, genre)
        check_success(success, "genre added successfully", "failed to add genre")
    elif command == "deleteViewer" and len(sys.argv) == 3:
        uid = sys.argv[2]
        success = t.delete_viewer(uid)
        check_success(success, "viewer deleted successfully", "failed to delete viewer")
    elif command == "insertMovie" and len(sys.argv) == 4:
        rid = sys.argv[2]
        url = sys.argv[3]
        success = t.insert_movie(rid, url)
        check_success(success, "movie url added successfully", "failed to add movie")
    elif command == "popularRelease" and len(sys.argv) == 3:
        # python project.py popularRelease 10
        number = sys.argv[2]
        success = b.top_release(number)
        check_success(success, "find the top release successfully", "fail to find top release")
    elif command == "releaseTitle" and len(sys.argv) == 3:
        success = b.find_release(sys.argv[2])
        check_success(success, "find releaseTitle successfully", "fail to find releaseTitle")
    elif command == "activeViewer" and len(sys.argv) == 5:
        #python project.py activeViewer 1 2025-01-01 2025-01-20
        try:
            times = int(sys.argv[2])
            success = b.find_viewers(times, sys.argv[3], sys.argv[4])
        except ValueError as e:
            success = false
            print("Error: " +e)
        check_success(success, "successfully shown active viewers", "fail to find active viewers")
    elif command == "videosViewed" and len(sys.argv) == 3:
        #python project.py videosViewed 123
        success = b.video_viewed(sys.argv[2])
        check_success(success, "successfully find video information", "fail to find video viewed info")



if __name__ == "__main__":
    main()
