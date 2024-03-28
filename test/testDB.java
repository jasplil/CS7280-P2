import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;

public class testDB {
    DB db;
    @Before
    public void setUp() throws FileNotFoundException {
        db = new DB("test.db0");
        db.createDB();
    }

    @Test
    public void testHeaderBlock() {
        try (RandomAccessFile file = new RandomAccessFile("test.db0", "r")) {
            String dbName = "DataBus";
            byte[] bytes = dbName.getBytes(StandardCharsets.UTF_8);
            file.readFully(bytes); // Read the first 8 bytes where the dbName is supposed to be

            // Convert bytes to String, assuming UTF-8 encoding, and trim it
            String storedDbName = new String(bytes, StandardCharsets.UTF_8).trim();

            // Find the database size
            file.seek(20); // Move the file pointer to the end of the database name
            int dbSize = file.readInt(); // Read the database size

            assertEquals("DataBus", storedDbName);
            assertEquals(1_048_576, dbSize);
        } catch (IOException e) {
            System.err.println("An error occurred while reading the database name: " + e.getMessage());
        }
    }

    @Test
    public void testWrite() throws IOException, URISyntaxException {
        db.write();
    }
}
