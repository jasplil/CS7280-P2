**Author**: Ting Tang & Wenqing Wu

# Project 2
## Functions that this database supports
- Write data
- Read data
- Download data file from NoSQL database
- Delete data file from NoSQL database
- Kill NoSQL database file
- Update bitmap after writing data
- Handle second data file Extent when full

## Key commands
```
open <filename>
close
write <data>
read <data>
download <filename>
delete <filename>
dir
```
## Data persistence

The data is stored to disk, the database file can be expanded to multiple data files. The data is stored in the data file, and the bitmap is used to indicate whether the data block is used. The B+tree is used to store the data physical address and the corresponding data file number.
## Write data

When writing data, the program will first check if the data file is full. If it is full, the program will create a new data file and write the data to the new data file. If the data file is not full, the program will write the data to the current data file. After writing the data, the program will update the bitmap to indicate that the data block is used. Meanwhile, a B+tree is maintained to store the data physical address and the corresponding data file number.

## Read data
![Design](https://github.com/jasplil/CS7280-P1/assets/39994190/a8a39633-dfce-4837-b643-681620336b97)
When reading data, the program will search in the B+tree to find the physical address of the data. Then the program will read the data from the data file according to the physical address.

## Database expansion
When the data file is full, the program will create a new data file and write the data to the new data file. The program will update the bitmap to indicate that the data block is used. Meanwhile, the program will update the B+tree to store the data physical address and the corresponding data file number.

### TODO

- [x] Update bitmap after writing data
- [x] Download data file from NoSQL database
- [x] Kill NoSQL database file
- [x] Delete data file from NoSQL database file




