// TODO: 1. Create a file to store data
//  2. Use methods in Utils.Block class to allocate and deallocate space
//  3. Use methods in B+Tree class to store and retrieve data

import Utils.Block;

import java.util.ArrayList;
import java.util.Arrays;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class DB {
    private File DBFile;
    private int TOTAL_SIZE; // 1 MB
    private Block[] blocks;

    public DB(String filename) {
        DBFile = new File(filename);
        TOTAL_SIZE = 1_048_576;
        // TODO: Change the size later
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
        int offset = 0;
        String dbName = "DataBus";

        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.putInt(TOTAL_SIZE);
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
            raf.seek(offset);
            raf.write(sizeBytes);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // TODO: Close - Closes the db connection
    public void close() {}

    public void write() throws IOException {
        InputStream is = getClass().getResourceAsStream("ratings.csv");
        BufferedReader csvFile = new BufferedReader(new InputStreamReader(is));
        RandomAccessFile raf = new RandomAccessFile(DBFile, "rw");

        int startingByte = 100;
        int countBlocks = 0;
        int totalBytes = 0;
        int itemNumber = 0;
        String dataRow = csvFile.readLine(); // Read first line.
        String[] blockArr = new String[] {};
        List<String> blockArray = new ArrayList<>(Arrays.asList(blockArr));

        for (int i = 0; i < 6; i++) {
            String[] dataArray = dataRow.split("\t");

            for (String item : dataArray) {
                blockArray.add(item);
            }

            dataRow = csvFile.readLine(); // Read next line of data.

            raf.seek(startingByte);
            for (String str : blockArray) {
                System.out.print(str + " "); // Print the data.
                raf.writeUTF(str); // Write the data to the file.
            }
        }
    }

    // TODO: Read - Reads the next line of data from the db
    public String search() {
        return "";
    }

    // TODO: Delete - Deletes the given data from the db
    public void delete(String data) {
        // TODO: delete the data from the b+tree

        // TODO: delete the records from block and free the space
    }
}
