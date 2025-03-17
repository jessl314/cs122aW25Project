import sys
import importdata as i
import task345 as t

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

if __name__ == "__main__":
    main()
