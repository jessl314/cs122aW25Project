# import sys
import sys
import importdata as i

def handle_import(folder_name):
    """Handle the 'import' command."""
    success = i.import_data(folder_name)
    if success:
        print("Data imported successfully.")
    else:
        print("Failed to import data.")

# def handle_command(function_name, *args):
#     try

def main():
    if len(sys.argv) < 2:
        print("Usage: python project.py <command> [args]")
        sys.exit(1)
    print('hi')
    command = sys.argv[1]
    if command == 'import' and len(sys.argv) == 3:
        folder_name = sys.argv[2]
        handle_import(folder_name)

if __name__ == "__main__":
    main()