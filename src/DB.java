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
    private Block[] blocks;
    String DB_NAME = "DataBus";
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
                insertFCB(raf, 80);      // Second operation
                // TODO: complete insertBitmap method
                // 3rd operation
                // bitmap
            } finally {
                lock.unlock(); // Ensure the lock is always released
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void insertMetadata(RandomAccessFile raf, int offset) throws IOException {
        // DB_NAME
        raf.seek(0);
        raf.writeUTF(DB_NAME);
        offset += 50;
        raf.seek(offset);
        // TOTAL_SIZE
        raf.writeInt(TOTAL_SIZE);
        offset += 9;
        raf.seek(offset);
        // # PFS
        raf.writeInt(2);
        offset += 2;
        raf.seek(offset);
        // Block size
        raf.writeInt(256);
        offset += 10;
        raf.seek(offset);
        raf.writeInt(1);
        // 71 bytes
    }

    public void insertFCB(RandomAccessFile raf, int offset) throws IOException {
        raf.seek(offset);
        raf.writeInt(1);
        raf.seek(offset + 3);
        raf.writeInt(256);
        raf.writeInt(1);
    }

    //
    public void write() throws IOException {
        InputStream is = getClass().getResourceAsStream("movies.csv");
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
