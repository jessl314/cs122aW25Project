# cs122aW25Project


THINGS TO DO:

everyone:

add a .env file to your root directory and put the following in with your user and password ( I need to grant you permission)

DB_HOST=localhost
DB_USER=
DB_PASSWORD=
DB_NAME=ZotStreamingcs122a


permission instructions: 
- replace jessica with your username lol

CREATE USER 'jessica'@'%' IDENTIFIED BY 'password';
GRANT ALL PRIVILEGES ON cs122a.* TO 'jessica'@'%';
FLUSH PRIVILEGES;

run this in a query in mysql

SET GLOBAL local_infile = 1;
SHOW VARIABLES LIKE 'local_infile'; // to check


AT THE END: change all permissions or whatever to reflect the stuff given in the instructions and hope nothing gets messed up 