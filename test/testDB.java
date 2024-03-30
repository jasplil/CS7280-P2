import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URISyntaxException;

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
    public void testWrite() throws IOException, URISyntaxException {
        db.write();
    }
}
