package Utils;

import java.util.List;

public class Block {
    final int SIZE = 256;
    private String[] data;
    private boolean isAllocated;

    public Block() {
        data = new String[SIZE];
        this.isAllocated = false;
    }

    public boolean isAllocated() {
        return isAllocated;
    }

    public void setAllocated(boolean allocated) {
        isAllocated = allocated;
    }
}
