// TODO: 1. Create a file to store data
//  2. Use methods in Block class to allocate and deallocate space
//  3. Use methods in B+Tree class to store and retrieve data

public class DB {

    public DB() {
    }

    // TODO: Open - Opens a new db connection at the file path
    public void open(String filePath) {}

    // TODO: Close - Closes the db connection
    public void close() {}

    // TODO: Read - Reads the next line of data from the db
    public String read() {
        // TODO: Look for the key in b+tree
        return "";
    }

    // TODO: Write - Writes the given data to the db
    public void write(String data) {
        // TODO: insert the data into the b+tree

        // TODO: update block size
    }

    // TODO: Delete - Deletes the given data from the db
    public void delete(String data) {
        // TODO: delete the data from the b+tree

        // TODO: delete the records from block and free the space
    }
}
