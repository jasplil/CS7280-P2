import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URISyntaxException;
import Utils.Bitmap;

import static org.junit.Assert.assertEquals;

public class testDB {
    DB db;
    @Before
    public void setUp() throws FileNotFoundException {
        db = new DB();
    }

    @Test
    public void testCreateDB() throws FileNotFoundException {
        db.open("test.db0");
    }

    @Test
    public void testHeaderBlock() {
        try (RandomAccessFile file = new RandomAccessFile("test.db0", "r")) {
            file.seek(0);
            String dbName = file.readUTF();
            assertEquals("MoviesDB", dbName);
            file.seek(50);
            int totalSize = file.readInt();
            assertEquals(1_048_576, totalSize);
        } catch (IOException e) {
            System.err.println("An error occurred while reading the database name: " + e.getMessage());
        }
    }

    @Test
    public void testPut() throws IOException, URISyntaxException {
        db.put("movies.csv");
    }

    @Test
    public void testClose() {
        db.close();
    }

    @Test
    public void testFind() throws IOException, URISyntaxException {
        System.out.println(db.find(15));
    }

    @Test
    public void testBitmap() {
        Bitmap blockBitmap = new Bitmap(40000);

        blockBitmap.setUsed(0);   // Mark block 0 as used
        blockBitmap.setUsed(39999); // Mark block 39999 as used

        boolean isUsed = blockBitmap.isUsed(0); // Check if block 0 is used
        assertEquals(true, isUsed);
        isUsed = blockBitmap.isUsed(39999); // Check if block 39999 is used
        assertEquals(true, isUsed);
        isUsed = blockBitmap.isUsed(1); // Check if block 1 is used
        assertEquals(false, isUsed);

        blockBitmap.setFree(0); // Mark block 0 as free
        isUsed = blockBitmap.isUsed(0); // Check if block 0 is used
        assertEquals(false, isUsed);

        // print the bitmap
        blockBitmap.print();
    }

    @Test
    public void testWriteCSV() throws IOException, URISyntaxException {
        db.download_csv();
    }

    @Test
    public void testGetDir() throws IOException, URISyntaxException {
        db.getDir();
    }
}
