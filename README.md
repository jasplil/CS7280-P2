**Author**: Ting Tang & Wenqing Wu

# Project 2 - NoSQL Database
## Functions that this database supports
- Write data
- Read data
- Download data file from NoSQL database
- Delete data file from NoSQL database
- Directory listing
- Kill NoSQL database file
- Update bitmap after writing data
- Handle second data file Extent when full

## Key commands
Add csv file under src folder, then run the following commands:
```
cd src  // go to src folder

Compile:
javac DB.java

Run:
Initialize a new database file: 
java DB open <DB FILE_NAME>

Write data:
java DB put movies.csv

Find kv pair:
java DB find 15

List all files:
java DB dir

Download file:
java DB get

Delete file:
java DB delete <DB FILE_NAME>

Close database:
java DB close <DB FILE_NAME>
```
## Data persistence

The data is stored to disk, the database file can be expanded to multiple data files. The data is stored in the data file, and the bitmap is used to indicate whether the data block is used. The B+tree is used to store the data physical address and the corresponding data file number.

The metadata is stored in the metadata file, which includes the data file number, the data block size, the bitmap size, the B+tree size, the data file size, the data file extent size, and the data file extent number.

The FCB is used to store the metadata file information, including the metadata file name, the metadata file size, the metadata file pointer, the metadata file extent size, and the metadata file extent number.
## Write data

When writing data, the program will first check if the data file is full. If it is full, the program will create a new data file and write the data to the new data file. If the data file is not full, the program will write the data to the current data file. After writing the data, the program will update the bitmap to indicate that the data block is used. Meanwhile, a B+tree is maintained to store the data physical address and the corresponding data file number.

## Read data
![Design](https://github.com/jasplil/CS7280-P1/assets/39994190/a8a39633-dfce-4837-b643-681620336b97)
When reading data, the program will search in the B+tree to find the physical address of the data. Then the program will read the data from the data file according to the physical address.
Each block is 256 bytes, and the data is stored in the block. The data is stored in the form of key-value pairs. The key is the data file number and the physical address, and the value is the data.
The data is specially truncated to 40 bytes for ease of storage and reading. The data is stored in the form of a string, and the data is separated by commas.





