// TODO: 1. Create a file to store data
//  2. Use methods in Utils.Block class to allocate and deallocate space
//  3. Use methods in B+Tree class to store and retrieve data

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;

public class DB {
    private File DBFile;

    public DB(String filename) {
        DBFile = new File(filename);
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
        int offset = 0;
        String dbName = "DataBus";
        int dbSize = 1_048_576; // 1 MB in bytes

        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.putInt(dbSize);
        byte[] sizeBytes = buffer.array();

        try {
            RandomAccessFile raf = new RandomAccessFile(DBFile, "rw");
            // Ensure that the database name does not exceed the allocated space
            byte[] dbNameBytes = dbName.getBytes(StandardCharsets.UTF_8);
            if (dbNameBytes.length > 50) {
                // If longer, truncate the database name to fit the allocated space
                dbNameBytes = Arrays.copyOf(dbNameBytes, 50);
            }

            // Prepare a header block with the specified size, initially filled with zeros
            byte[] headerBlock = new byte[256];
            // Copy the database name into the beginning of the header block
            System.arraycopy(dbNameBytes, 0, headerBlock, 0, dbNameBytes.length);

            // Write the header block to the beginning of the file
            raf.seek(0); // Position the file pointer at the start of the file
            raf.write(headerBlock);
            offset += 20;
            System.out.println("Offset: " + offset);
            raf.seek(offset);
            raf.write(sizeBytes);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // TODO: Close - Closes the db connection
    public void close() {}

    // TODO: Read - Reads the next line of data from the db
    public String read() {
        // TODO: Look for the key in b+tree
        return "";
    }

    // TODO: Write - Writes the given data to the db
    public void write(String data) {
        // TODO: Reserve space for the header
        // TODO: Write the data to the block

    }

    // TODO: Delete - Deletes the given data from the db
    public void delete(String data) {
        // TODO: delete the data from the b+tree

        // TODO: delete the records from block and free the space
    }
}
