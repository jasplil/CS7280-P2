// TODO: 1. Create a file to store data
//  2. Use methods in Utils.Block class to allocate and deallocate space
//  3. Use methods in B+Tree class to store and retrieve data

import Utils.Block;

import javax.management.relation.RelationNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Random;
import java.util.concurrent.locks.ReentrantLock;

public class DB {
    private File DBFile;
    private int TOTAL_SIZE; // 1 MB
    private String FILE_NAME = "movies.csv";
    private Block[] blocks;
    String DB_NAME = "MoviesDB";
    BPlusTree tree = new BPlusTree(6);

    public DB(String filename) {
        DBFile = new File(filename);
        TOTAL_SIZE = 1_048_576;
        // TODO: db extension: db1, db2, db3, ...
        blocks = new Block[300];

        try {
            // Attempt to create the file
            boolean fileCreated = DBFile.createNewFile();

            if (fileCreated) {
                System.out.println("File created successfully: " + DBFile.getAbsolutePath());
            } else {
                System.out.println("File already exists: " + DBFile.getAbsolutePath());
            }
        } catch (IOException e) {
            System.err.println("An error occurred while creating the file: " + e.getMessage());
        }
    }

    /**
     * Create a new database file. If the file already exists, do nothing.
     */
    public void createDB() throws FileNotFoundException {
        ReentrantLock lock = new ReentrantLock();

        try {
            RandomAccessFile raf = new RandomAccessFile(DBFile, "rw");
            // Ensure that the database name does not exceed the allocated space
            lock.lock(); // Acquire the lock
            try {
                // physical address
                int offset = 0;
                // TODO: complete insertMetadata
                insertMetadata(raf, offset); // First operation
                // TODO: complete insertFCB method
                insertFCB(raf, 256);      // Second operation
                // TODO: complete insertBitmap method
                // 3rd operationm
                // bitmap
            } finally {
                lock.unlock(); // Ensure the lock is always released
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void insertMetadata(RandomAccessFile raf, int offset) throws IOException {
        // Assuming the offset is 0 at the start.
        // DB_NAME with padding to 50 bytes
        raf.seek(offset);
        raf.writeUTF(DB_NAME);
        int bytesForName = 2 + DB_NAME.length(); // 2 bytes for UTF length
        padWithBytes(raf, 50 - bytesForName);

        offset += 50; // Now the offset is increased by the allocated space for DB_NAME
        raf.seek(offset);

        // TOTAL_SIZE is use 4 bytes, total_size = 1_048_576
        raf.writeInt(TOTAL_SIZE);

        offset += 4; // TOTAL_SIZE + padding
        raf.seek(offset);

        // # PFS with padding to 4 bytes (already an int, no padding needed if we're just writing the int)
        raf.writeInt(1);

        offset += 4; // # PFS
        raf.seek(offset);

        // Block size with padding to 4 bytes (already an int, no padding needed if we're just writing the int)
        raf.writeInt(256);

        offset += 4; // Block size
        raf.seek(offset);

        // # of uploaded csv files with padding to 4 bytes (already an int, no padding needed if we're just writing the int)
        raf.writeInt(1);

        offset += 4; // # of uploaded csv files
        raf.seek(offset); // Now at 66 bytes, but if you want to pad this to 256 as well
        padWithBytes(raf, 256 - offset);
    }

    // Helper method to write padding bytes
    private void padWithBytes(RandomAccessFile raf, int numberOfBytes) throws IOException {
        for (int i = 0; i < numberOfBytes; i++) {
            raf.writeByte(0);
        }
    }


    public void insertFCB(RandomAccessFile raf, int offset) throws IOException {
        raf.seek(offset); // Move to the correct start position, which is 256

        // File Name with padding to 50 bytes
        raf.writeUTF(FILE_NAME);
        int bytesForName = 2 + FILE_NAME.length(); // 2 bytes for UTF length
        padWithBytes(raf, 50 - bytesForName);

        offset += 50; // Now the offset is increased by the allocated space for DB_NAME
        raf.seek(offset);

        //File Size: fileSizeInBytes
        long fileSizeInBytes = calculateFileSize();
        raf.writeLong(fileSizeInBytes);
        offset += 8; // File Size
        raf.seek(offset);

        // Date and time: date and time when the file was uploaded
        raf.writeUTF("2024-03-22, 23:59:59");
        offset += 20; // Date and time
        raf.seek(offset);

        // Starting block(location of first data block): 4 bytes
        raf.writeInt(256 * 3); // 256 * 3 + 1
        offset += 4;
        raf.seek(offset);

        // ending block(location of last data block): 4 bytes, just assume same as starting block when intializing
        raf.writeInt(256 * 19999); // 256 * 3 + 1
        offset += 4;
        raf.seek(offset);
        
        // # number of blocks used: 4 bytes
        raf.writeInt(6); // 6 blocks used, at the very beginning, three blocks are used for metadata, FCB, and bitmap
        // 3 blocks are used for the B+ tree, change it later, this is dummy data
        offset += 4;
        raf.seek(offset);
        
        // starting address of the B+ tree index: 4 bytes
        raf.writeInt(256 * 20000); // 256 * 3 + 1
        offset += 4;
        raf.seek(offset);


        // Now, if you want the FCB section to occupy exactly 256 bytes, you'll need to add padding
        int bytesWritten = 94; // Total bytes actually written for FCB data
        int paddingSize = 256 - bytesWritten; // Calculate padding needed to reach 256 bytes
        // Write padding bytes
        for (int i = 0; i < paddingSize; i++) {
            raf.writeByte(0); // Using 0 for padding, but you could use another value
        }

    }

    // helper method to calculate the input csv file size in bytes
    public long calculateFileSize() {
        File file = new File(getClass().getResource("/movies.csv").getFile());
        long fileSizeInBytes = 0;
        if (file.exists()) {
            fileSizeInBytes = file.length();
            System.out.println("File size: " + fileSizeInBytes + " bytes");
        } else {
            System.out.println("The inout cvs file does not exist.");
        }
        return fileSizeInBytes;
    }

    public void insertBitmap(RandomAccessFile raf, int offset) throws IOException {
        raf.seek(offset);
        raf.writeBoolean(false);
    }


    //
    public void write() throws IOException {
        InputStream is = getClass().getResourceAsStream(FILE_NAME);
        BufferedReader csvFile = new BufferedReader(new InputStreamReader(is));
        RandomAccessFile raf = new RandomAccessFile(DBFile, "rw");

        int startingByte = 256 * 3 + 1;
        String dataRow = csvFile.readLine(); // Read first line.
        dataRow = csvFile.readLine(); // Read second line.
        int totalBytes = 256;
        raf.seek(startingByte);
        for (int i = 0; i < 6 && dataRow != null; i++) {
            int byteLength = dataRow.getBytes().length;
            if (byteLength > 40) {
                dataRow = truncateString(dataRow);
            }
            raf.writeUTF(dataRow);
            // 1. Toy Story (1995) -> 1, Toy Story (1995)
            int index = Integer.parseInt(dataRow.split(",")[0]);
            // TODO: write b+tree to file
            // TODO: convert the tree node to byte array
            tree.insert(index, startingByte);
            // IMPORTANT: Update startingByte for next write, considering the length of the UTF string (dataRow) and 2 bytes for length
            startingByte += dataRow.getBytes(StandardCharsets.UTF_8).length + 2;
            System.out.println("Index: " + index+1 + " | Byte: " + startingByte);
            dataRow = csvFile.readLine(); // Read next line of data.
        }

        // TODO: move to writeBTreeToFile
        // Search the data from the file
        int numBlocks = (int) Math.ceil(tree.search(6));
        RandomAccessFile file = new RandomAccessFile(DBFile, "r");
        System.out.println("Address: " + numBlocks);
        file.seek(numBlocks);
        String data = file.readUTF();
        System.out.println(data);
    }

    public static String truncateString(String str) {
        byte[] strBytes = str.getBytes(StandardCharsets.UTF_8);
        if (strBytes.length > 40) {
            // Find the maximum length in characters that fits within maxSizeInBytes
            int newLength = 40;
            while (newLength > 0) {
                String substring = new String(strBytes, 0, newLength, StandardCharsets.UTF_8);
                if (substring.getBytes(StandardCharsets.UTF_8).length <= 40) {
                    return substring;
                }
                newLength--;
            }
        }
        return str; // Return the original string if it doesn't exceed the size limit
    }

    // TODO: Write - Writes the btree to the end of file
    // Convert the tree node to byte array
    public void writeBTreeToFile() {}

    // TODO: Read - Reads the next line of data from the db
    public String search() {
        // TODO: search the data from the b+tree

        return "";
    }

    // TODO: Delete - Deletes the given data from the db
    public void delete(String data) {
        // TODO: delete the data from the b+tree

        // TODO: delete the records from block and free the space
    }

    // TODO: Close - Closes the db connection
    public void close() {
        // Close the file
    }
}
