package Utils;

public class Block {
    private Block prev;
    private Block next;
    private boolean isAllocated;

    public Block(Block prev, Block next) {
        this.prev = prev;
        this.next = next;
    }

    public boolean isAllocated() {
        return isAllocated;
    }

    public void setAllocated(boolean allocated) {
        isAllocated = allocated;
    }

    public Block getNext() {
        return next;
    }

    public void setNext(Block next) {
        this.next = next;
    }

    public Block getPrev() {
        return prev;
    }

    public void setPrev(Block prev) {
        this.prev = prev;
    }
}
