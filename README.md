**Author**: Ting Tang & Wenqing Wu

# Project 2

## Write data

When writing data, the program will first check if the data file is full. If it is full, the program will create a new data file and write the data to the new data file. If the data file is not full, the program will write the data to the current data file. After writing the data, the program will update the bitmap to indicate that the data block is used. Meanwhile, a B+tree is maintained to store the data physical address and the corresponding data file number.

## Read data

When reading data, the program will search in the B+tree to find the physical address of the data. Then the program will read the data from the data file according to the physical address.


### TODO

- [x] Update bitmap after writing data
- [x] Download data file from NoSQL database
- [x] Handle second data file Extent when full
- [x] Kill NoSQL database file
- [x] Delete data file from NoSQL database file
- [x] DIRectory listing
- [x] Finished details for btree serialization
- [x] Writeup




