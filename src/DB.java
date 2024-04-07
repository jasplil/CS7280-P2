// TODO: 1. Create a file to store data
//  2. Use methods in Utils.Block class to allocate and deallocate space
//  3. Use methods in B+Tree class to store and retrieve data

import Utils.Block;

import javax.management.relation.RelationNotFoundException;
import java.util.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.locks.ReentrantLock;

public class DB {
    private File DBFile;
    private int TOTAL_SIZE; // 1 MB
    private String FILE_NAME = "movies.csv";
    private Block[] blocks;
    HashMap<String, int[]> dataMap = new HashMap<>();

    String DB_NAME = "MoviesDB";
    BPlusTree tree = new BPlusTree(200);
    RandomAccessFile raf;

    public DB(String filename) {
        DBFile = new File(filename);
        TOTAL_SIZE = 1_048_576;
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
            raf = new RandomAccessFile(DBFile, "rw");
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
                // 3rd operation
                insertBitmap(raf, 256 * 2); // Third operation
                // bitmap

                // TODO: move the following code to a fcb
                dataMap.put("test.db0", new int[]{0, 10000});
                dataMap.put("test.db1", new int[]{10000, 20000});
                dataMap.put("test.db2", new int[]{20001, 30000});
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

        offset += paddingSize;
        raf.seek(offset); // Now at 256 bytes, ready for the next operation
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

    // this is based on the movies_large file records, I want to create around 40,000 blocks to store the data
    public void insertBitmap(RandomAccessFile raf, int offset) throws IOException {
        // Seek to the offset position where the bitmap starts
        raf.seek(offset);

        // Each bit in the bitmap represents a block, so we need 5,000 bytes for 40,000 blocks
        // But since we want the bitmap to be a multiple of 256 bytes, we round up to 5,120 bytes
        int bitmapSize = 5120; // This is 20 blocks, each block is 256 bytes
        byte[] bitmapBytes = new byte[bitmapSize]; // Initialized to all zeroes

        // Write the bitmap to the file
        raf.write(bitmapBytes);
        offset += bitmapSize;
        raf.seek(offset);
        // Now the file pointer in 'raf' is at offset + bitmapSize, ready for the next operation
    }


    public void write() throws IOException {
        InputStream is = getClass().getResourceAsStream(FILE_NAME);
        BufferedReader csvFile = new BufferedReader(new InputStreamReader(is));
        // Check which db file to write to
        RandomAccessFile raf = new RandomAccessFile(DBFile, "rw");

        // Start after bitmap
        int startingByte = 256 * 22;
        String dataRow = csvFile.readLine(); // Read first line.
        dataRow = csvFile.readLine(); // Read second line.
        raf.seek(startingByte);
//        while (dataRow != null)
        for (int i = 0; i < 9000 && dataRow != null; i++) {
            int byteLength = dataRow.getBytes().length;
            if (byteLength > 40) {
                dataRow = truncateString(dataRow);
            }
            raf.writeUTF(dataRow);
            // 1. Toy Story (1995) -> 1, Toy Story (1995)
            int index = Integer.parseInt(dataRow.split(",")[0]);
            tree.insert(index, startingByte);
            // IMPORTANT: Update startingByte for next write, considering the length of the UTF string (dataRow) and 2 bytes for length
            startingByte += dataRow.getBytes(StandardCharsets.UTF_8).length + 2;
            dataRow = csvFile.readLine(); // Read next line of data.
        }

        int endingByte = startingByte;
        System.out.println("Ending byte: " + endingByte);
        writeBTreeToFile(endingByte);
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

    // Convert the tree node to byte array
    public void writeBTreeToFile(int startingByte) throws IOException {
        tree.writeBPlusTreeToFile(tree, "test.db0", startingByte);
    }

    public String search() throws IOException {
        BPlusTree deserializedTree = tree.readBPlusTreeFromFile("test.db0", 394329);
        double address = deserializedTree.search(193609);
        raf.seek((int) address);
        System.out.println(raf.readUTF());
        return raf.readUTF();
    }

    // TODO: Delete - Deletes FCB
    public void delete(String data) {
    }

    // TODO: Close - Closes the db connection
    public void close() {
        // Close the file
    }
}
