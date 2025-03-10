import os
import mysql.connector
from dotenv import load_dotenv

# QUESTION: is it ok to lower all table names
# NOTE: make sure to change the database name to cs122a before submission

load_dotenv()

db_host = os.getenv('DB_HOST', 'localhost')
db_user = os.getenv('DB_USER', 'root')
db_password = os.getenv('DB_PASSWORD', '')
db_name = os.getenv('DB_NAME', 'cs122a')

def create_connection():
    """ creates the database connection using .env or default credentials"""
    try:
        connection = mysql.connector.connect(
            host=db_host,
            user=db_user,
            password=db_password,
            database=db_name,
            allow_local_infile=True
        )
        return connection
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None

def create_tables():
    """executing the DDL statements to create the tables. returns True if this was successful, False otherwise"""
    connection = create_connection()
    if not connection:
        return False
    cursor = connection.cursor()

    create_users_table = """
    CREATE TABLE IF NOT EXISTS users (
    uid INT,
    email TEXT NOT NULL,
    joined_date DATE NOT NULL,
    nickname TEXT NOT NULL,
    street TEXT,
    city TEXT,
    state TEXT,
    zip TEXT,
    genres TEXT,
    PRIMARY KEY (uid)
    );
    """
    create_producers_table = """
    CREATE TABLE producers (
    uid INT,
    bio TEXT,
    company TEXT,
    PRIMARY KEY (uid),
    FOREIGN KEY (uid) REFERENCES users(uid) ON DELETE CASCADE
    );
    """
    create_viewers_table = """
    CREATE TABLE viewers (
    uid INT,
    subscription ENUM('free', 'monthly', 'yearly'),
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    PRIMARY KEY (uid),
    FOREIGN KEY (uid) REFERENCES users(uid) ON DELETE CASCADE
    );
    """
    create_releases_table = """
    CREATE TABLE releases (
        rid INT,
        producer_uid INT NOT NULL,
        title TEXT NOT NULL,
        genre TEXT NOT NULL,
        release_date DATE NOT NULL,
        PRIMARY KEY (rid),
        FOREIGN KEY (producer_uid) REFERENCES producers(uid) ON DELETE CASCADE
    );
    """
    create_movies_table = """
    CREATE TABLE movies (
    rid INT,
    website_url TEXT,
    PRIMARY KEY (rid),
    FOREIGN KEY (rid) REFERENCES releases(rid) ON DELETE CASCADE
    );
    """
    create_series_table = """
    CREATE TABLE series (
        rid INT,
        introduction TEXT,
        PRIMARY KEY (rid),
        FOREIGN KEY (rid) REFERENCES releases(rid) ON DELETE CASCADE
    );
    """
    create_videos_table = """
    CREATE TABLE videos (
        rid INT,
        ep_num INT NOT NULL,
        title TEXT NOT NULL,
        length INT NOT NULL,
        PRIMARY KEY (rid, ep_num),
        FOREIGN KEY (rid) REFERENCES releases(rid) ON DELETE CASCADE
    );
    """
    create_sessions_table = """
    CREATE TABLE Sessions (
        sid INT,
        uid INT NOT NULL,
        rid INT NOT NULL,
        ep_num INT NOT NULL,
        initiate_at DATETIME NOT NULL,
        leave_at DATETIME NOT NULL,
        quality ENUM('480p', '720p', '1080p'),
        device ENUM('mobile', 'desktop'),
        PRIMARY KEY (sid),
        FOREIGN KEY (uid) REFERENCES viewers(uid) ON DELETE CASCADE,
        FOREIGN KEY (rid, ep_num) REFERENCES videos(rid, ep_num) ON DELETE CASCADE
    );
    """
    create_reviews_table = """
    CREATE TABLE Reviews (
    rvid INT,
    uid INT NOT NULL,
    rid INT NOT NULL,
    rating DECIMAL(2, 1) NOT NULL CHECK (rating BETWEEN 0 AND 5),
    body TEXT,
    posted_at DATETIME NOT NULL,
    PRIMARY KEY (rvid),
    FOREIGN KEY (uid) REFERENCES viewers(uid) ON DELETE CASCADE,
    FOREIGN KEY (rid) REFERENCES releases(rid) ON DELETE CASCADE
    );
    """
    try:
        cursor.execute(create_users_table)
        cursor.execute(create_producers_table)
        cursor.execute(create_viewers_table)
        cursor.execute(create_releases_table)
        cursor.execute(create_movies_table)
        cursor.execute(create_series_table)
        cursor.execute(create_videos_table)
        cursor.execute(create_sessions_table)
        cursor.execute(create_reviews_table)
        print("Tables created successfully.")
    except mysql.connector.Error as err:
        print(f"Error creating tables: {err}")
        return False
    finally:
        connection.commit()
        cursor.close()
        connection.close()
    return True

def load_data_from_csv(file_path, table_name):
    """Loads data from file_path specified into the correct table."""
    # Convert to absolute path and replace `\` with `/` for MySQL compatibility
    absolute_path = os.path.abspath(file_path).replace("\\", "/")

    print(f"Attempting to load {absolute_path} into {table_name}")  # Debugging

    connection = create_connection()
    if not connection:
        return False
    cursor = connection.cursor()

    try:
        # Disable foreign key checks temporarily
        cursor.execute("SET FOREIGN_KEY_CHECKS=0;")

        load_query = f"""
        LOAD DATA LOCAL INFILE '{absolute_path}'
        INTO TABLE {table_name}
        FIELDS TERMINATED BY ',' 
        LINES TERMINATED BY '\n' 
        IGNORE 1 ROWS;
        """

        cursor.execute(load_query)

        # Re-enable foreign key checks
        cursor.execute("SET FOREIGN_KEY_CHECKS=1;")

        print(f"Data from {absolute_path} loaded into {table_name}")

        # Check how many rows were inserted
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]
        print(f"✅ {row_count} rows now exist in {table_name}.")

    except mysql.connector.Error as err:
        print(f"Error loading data: {err}")
        return False
    finally:
        connection.commit()
        cursor.close()
        connection.close()
   
    return True

def reset_database():
    """resets the database to ensure clean state."""
    connection = create_connection()
    if not connection:
        return False
    cursor = connection.cursor()
    try:
        cursor.execute("DROP DATABASE IF EXISTS ZotStreamingcs122a;")
        cursor.execute("CREATE DATABASE ZotStreamingcs122a")
        cursor.execute("USE ZotStreamingcs122a")
        print("Database reset successfully")
    except mysql.connector.Error as err:
        print(f"Error creating tables: {err}")
        return False
    finally:
        cursor.close()
        connection.close()
    return True

# folder name is the argument test_data for the import statement
#CHANGE noted below, actually maybe ask ED, maybe not?

def import_data(folder_name):
    """
    Iterates through the correct test data folder and loads the CSV files into the database.
    """
    print(f"Checking folder: {folder_name}")  # Debugging

    # Ensure the folder exists
    if not os.path.exists(folder_name):
        print(f"Error: Folder '{folder_name}' does not exist!")
        return False

    # Fix: Select the correct `test_data/` folder
    correct_folder = folder_name

    if os.path.exists(os.path.join(folder_name, "movies.csv")):
        print(f"Using folder: {folder_name}")  # Debugging
    elif os.path.exists(os.path.join(folder_name, "test_data", "movies.csv")):
        correct_folder = os.path.join(folder_name, "test_data")
        print(f"Using nested folder: {correct_folder}")  # Debugging
    else:
        print("Error: Could not find valid CSV files in test_data.")
        return False

    # List files in the correct folder
    files = os.listdir(correct_folder)
    print(f"Files found in '{correct_folder}': {files}")  # Debugging

    # Filter only valid CSV files (exclude hidden/macOS files)
    csv_files = [f for f in files if f.endswith('.csv') and not f.startswith('.')]

    if not csv_files:
        print(f"Error: No valid CSV files found in '{correct_folder}'")
        return False

    # Ensure database is reset
    if not reset_database():
        print("Database reset failed")
        return False
    if not create_tables():
        print("Table creation failed")
        return False

    connection = create_connection()
    if not connection:
        print("Database connection failed")
        return False
    cursor = connection.cursor()

    for csv_file in csv_files:
        file_path = os.path.join(correct_folder, csv_file)
        table_name = csv_file.replace(".csv", "")

        print(f"Processing file: {file_path} -> Table: {table_name}")  # Debugging

        success = load_data_from_csv(file_path, table_name)
        if not success:
            print(f"Failed to load {file_path} into {table_name}")
            return False

    connection.commit()
    cursor.close()
    connection.close()

    print("Data imported successfully.")
    return True

