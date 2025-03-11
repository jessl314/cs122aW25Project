import sys
import importdata as i
import task3 as t

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

if __name__ == "__main__":
    main()
