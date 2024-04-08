import java.time.Instant;

import Utils.Bitmap;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.locks.ReentrantLock;

import java.io.FileWriter;
import java.io.IOException;

/**
 * A simple database class that uses a B+ tree to store data.
 */
public class DB {
    private File DBFile;
    private int TOTAL_SIZE; // 1 MB
    private String FILE_NAME = "movies.csv";

    String DB_NAME = "MoviesDB";
    BPlusTree tree = new BPlusTree(200);
    RandomAccessFile raf;

    public DB() {}

    /**
     * Initialize the database file.
     * Open the database file and write the metadata.
     */
    public void open(String DB_NAME) throws FileNotFoundException {
        DBFile = new File(DB_NAME);
        TOTAL_SIZE = 1_048_576;

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

        ReentrantLock lock = new ReentrantLock();

        try {
            raf = new RandomAccessFile(DBFile, "rw");
            lock.lock(); // Acquire the lock
            try {
                // physical address
                int offset = 0;
                insertMetadata(raf, offset); // First operation
                insertFCB(raf, 256);      // Second operation
                // 3rd operation
                insertBitmap(raf, 256 * 2); // Third operation
            } finally {
                lock.unlock(); // Ensure the lock is always released
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Insert metadata into the database file.
     */
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

    /**
     * Insert the File Control Block (FCB) into the database file.
     */
    public void insertFCB(RandomAccessFile raf, int offset) throws IOException {
        raf.seek(offset); // Move to the correct start position, which is 256

        // File Name with padding to 50 bytes
        raf.writeUTF(FILE_NAME);
        int bytesForName = 2 + FILE_NAME.length(); // 2 bytes for UTF length
        padWithBytes(raf, 50 - bytesForName);

        offset += 50; // Now the offset is increased by the allocated space for DB_NAME
        raf.seek(offset);
        int fileSizeInBytes = calculateFileSize();
        raf.write(fileSizeInBytes);

        offset += 8; // File Size
        raf.seek(offset);

        // Date and time: date and time when the file was uploaded
        Instant timestamp = Instant.now();
        raf.writeUTF(String.valueOf(timestamp));
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

    /**
     * Update the number of blocks used and the ending block address in the FCB section.
     */
    public void update_number_of_blocks_used_and_ending_block(RandomAccessFile raf, int blocksUsed, int EndingBlockAddress) throws IOException {
        int offset = 256 + 50 + 8 + 20 + 4;

        raf.seek(offset);
        raf.writeInt(EndingBlockAddress);
        offset += 4;
        raf.seek(offset);
        raf.writeInt(blocksUsed);
    }

    /**
     * Helper method to calculate the input csv file size in bytes.
     */
    public int calculateFileSize() {
        File file = new File("movies.csv");
        int fileSizeInBytes = 0;
        if (file.exists()) {
            System.out.println("The input csv file exists.");
            fileSizeInBytes = (int) file.length();
        } else {
            System.out.println("The input cvs file does not exist.");
        }
        return fileSizeInBytes;
    }

    /**
     * Insert the bitmap into the database file.
     */
    public void insertBitmap(RandomAccessFile raf, int offset) throws IOException {
        // Seek to the offset position where the bitmap starts
        raf.seek(offset);
        // Each bit in the bitmap represents a block, so we need 5,000 bytes for 40,000 blocks
        // But since we want the bitmap to be a multiple of 256 bytes, we round up to 5,120 bytes
        Bitmap bitmap = new Bitmap(40000); // Initializing the Bitmap to cover 40,000 blocks, assuming we start counting qfrom data block 0

        // Convert the Bitmap to a byte array (you need to adjust this method if your actual
        // usage scenario is different)
        byte[] bitmapBytes = bitmap.toByteArray(); // This should be the correct size, e.g., 20 bytes

        // Write the byte array to the file
        raf.write(bitmapBytes);
        offset += bitmapBytes.length; // Now the offset is increased by the size of the bitmap
        raf.seek(offset);
        // Now the file pointer in 'raf' is at offset + bitmapSize, ready for the next operation
    }

    /**
     * Update the bitmap after writing data blocks to the file.
     */
    public void updateBitmapAfterWritingFile(RandomAccessFile raf, int startingBlockUsed, int endingBlockUsed) throws IOException {
        int offset = 256 * 2; // The offset where the bitmap starts in the file
        raf.seek(offset);

        // Read the existing bitmap into a byte array
        byte[] bitmapBytes = new byte[5120]; // Size should match your bitmap size in the file
        raf.readFully(bitmapBytes);

        // Update the bitmap for the range of used blocks
        for (int blockNum = startingBlockUsed; blockNum <= endingBlockUsed; blockNum++) {
            int byteIndex = blockNum / 8;
            int bitIndex = blockNum % 8;
            bitmapBytes[byteIndex] |= (1 << bitIndex); // Set the bit to 1 to mark the block as used
        }

        // Write the updated bitmap back to the file
        raf.seek(offset); // Seek back to the start of the bitmap
        raf.write(bitmapBytes);

//        System.out.println("BitmapBytes: " + Arrays.toString(bitmapBytes));
//        0 in byte form is 00000000 in binary.
//        -64 in byte form is 11000000 in binary. The first bit in a byte is the sign bit in the two's complement binary representation Java uses for numbers. So, 11000000 represents the decimal number -64.
//        -1 in byte form is 11111111 in binary, which indicates that all the bits are set to 1 for that byte.
    }

    /**
     * Write the data from the csv file to the database file.
     */
    public void put(String FILE_NAME) throws IOException {
        InputStream is = getClass().getClassLoader().getResourceAsStream(FILE_NAME);
        BufferedReader csvFile = new BufferedReader(new InputStreamReader(is));
        RandomAccessFile raf = new RandomAccessFile("test.db0", "rw");
        // Start after bitmap
        int startingByte = 256 * 22;
        String dataRow = csvFile.readLine(); // Read first line.
        dataRow = csvFile.readLine(); // Read second line.
        raf.seek(startingByte);

        while (dataRow != null) {
            int byteLength = dataRow.getBytes().length;
            if (byteLength > 40) {
                dataRow = truncateString(dataRow);
            }
            raf.writeUTF(dataRow);
            int index = Integer.parseInt(dataRow.split(",")[0]);
            tree.insert(index, startingByte);
            // IMPORTANT: Update startingByte for next write, considering the length of the UTF string (dataRow) and 2 bytes for length
            startingByte += dataRow.getBytes(StandardCharsets.UTF_8).length + 2;
            dataRow = csvFile.readLine(); // Read next line of data.

            if (startingByte > 256 * 20000) {
                updateBitmapAfterWritingFile(raf, 20000, 20000 * 2);
            }
        }

        int endingByte = startingByte;
        writeBTreeToFile(256 * 20000);

        int EndingBlocksUsed = (int)Math.ceil(endingByte / 256);
        update_number_of_blocks_used_and_ending_block(raf, EndingBlocksUsed, endingByte);
        updateBitmapAfterWritingFile(raf, 22, EndingBlocksUsed);
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

    public String find(int searchKey) throws IOException {
        RandomAccessFile raf = new RandomAccessFile("test.db0", "rw");
        // check if FCB is deleted
        raf.seek(306);
        if (raf.read() < 1) {
            System.out.println("File has been deleted");
            return null;
        }

        BPlusTree deserializedTree = tree.readBPlusTreeFromFile("test.db0", 256 * 20000);

        double address = deserializedTree.search(searchKey);
        int block = (int)Math.ceil(address / 256);
        System.out.println("Current block is: " + Math.ceil(block));
        raf.seek((int) address);
        return raf.readUTF();
    }

    /**
     * Delete a FCB info from the database.
     */
    public void delete(String FILE_NAME) throws IOException {
        int fcbOffset = 256; // For example, if FCBs start at byte 256
        deleteFile(FILE_NAME, fcbOffset);
    }

    public void deleteFile(String FILE_NAME, int fcbOffset) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(FILE_NAME, "rw");
        raf.seek(fcbOffset);

        byte[] nullBytes = new byte[256]; // Create a byte array filled with 0s

        raf.seek(fcbOffset); // Move the file pointer to the specified offset
        raf.write(nullBytes); // Write the null bytes to the file

        raf.seek(306);

//        // Update the bitmap to free the blocks used by this file
        int bitmapOffset = 256 * 2; // For example, if the bitmap starts at byte 512
        raf.seek(bitmapOffset); // Move to the start position of the bitmap
//        // Read the existing bitmap into a byte array
//        byte[] bitmapBytes = new byte[5120]; // Size should match your bitmap size in the file
//        raf.readFully(bitmapBytes);
//        // TODO：update bitmapBytes to free the blocks used by the file, and the Btree index
//        raf.seek(bitmapOffset); // Move back to the start position of the bitmap
//        raf.write(bitmapBytes); // Write the updated bitmap back to the file

        System.out.println("FCB has been deleted successfully.");
    }

    public void close(String FILE_NAME) throws FileNotFoundException {
        RandomAccessFile raf = new RandomAccessFile(FILE_NAME, "rw");
        // Close the file
        try {
            if (raf != null) {
                raf.close(); // Close the RandomAccessFile stream
                System.out.println("Database connection closed successfully.");
            }
        } catch (IOException e) {
            System.err.println("An error occurred while closing the database connection: " + e.getMessage());
        }
    }

    public void download_csv() throws IOException {
        RandomAccessFile raf = new RandomAccessFile("test.db0", "r");
        int startingBlock = 256 * 22;  // The block where data starts (from raf = 256 * 22)
        int endingBlock = 256 * 19999; // The block where data ends (to raf = 256 * 19999)
        int recordSize = 40;           // Size of each record in bytes
        int recordsPerBlock = 6;       // Number of records per block

        // Path to the output CSV file
        String outputPath = "output_movies.csv"; // Saving the CSV in the 'test' directory

        // Open a FileWriter to write to a CSV file
        try (FileWriter csvWriter = new FileWriter(outputPath)) {
            for (int offset = startingBlock; offset <= endingBlock; offset += 256) { // iterate over blocks
                raf.seek(offset); // Move to the start of the block

                // Read records within the block
                for (int i = 0; i < recordsPerBlock; i++) {
                    byte[] recordBytes = new byte[recordSize];
                    if (raf.getFilePointer() + recordSize > raf.length()) {
                        // If trying to read past the end of the file, break the loop
                        break;
                    }
                    raf.readFully(recordBytes); // Read a record
                    String record = new String(recordBytes, StandardCharsets.UTF_8).trim(); // Convert bytes to String

                    // Write the record as a row in the CSV file, assuming records are newline separated
                    csvWriter.write(record + "\n");
                }
            }
        } catch (IOException e) {
            System.err.println("An error occurred while writing the CSV file: " + e.getMessage());
        }
    }

    /**
     * This method reads the directory and lists all the .db files in it.
     */
    public void getDir() throws IOException {
        RandomAccessFile raf = new RandomAccessFile("test.db0", "r");

        int csvOffset = 256;
        raf.seek(csvOffset);
        System.out.print("Name: " + raf.readUTF() + " ");

        int fileSizeOffset = 306;
        raf.seek(fileSizeOffset);
        System.out.print("Size: " + raf.readInt() + " bytes ");

        int timestampOffset = 314;
        raf.seek(timestampOffset);
        String timestampString = raf.readUTF();
        System.out.println("Time: " + timestampString);
    }

    public static void main(String[] args) throws IOException {
        DB db = new DB();
        switch (args[0].toLowerCase()) {
            case "open":
                db.open(args[1]);
                break;
            case "put":
                db.put(args[1]);
                break;
            case "find":
                System.out.println(db.find(Integer.parseInt(args[1])));
                break;
            case "dir":
                db.getDir();
                break;
            case "delete":
                db.delete(args[1]);
                break;
            case "get":
                db.download_csv();
                break;
            case "close":
                db.close(args[1]);
                break;
            default:
                System.out.println("Invalid command.");
        }
    }
}
