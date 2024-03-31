package Utils;//package Utils;
//
//import java.util.ArrayList;
//
//public class Bitmap {
//    public boolean[] bitmap;
//
//    Bitmap() {
//        bitmap = new boolean[256];
//
//        // empty disk
//        for (int i = 0; i < 256; i++)
//            bitmap[i] = false;
//    }
//
//    public int [] emptyBlocks(int blockLen) {
//        int [] emptyBlocks = new int[blockLen];
//        ArrayList<Integer> temp = new ArrayList<>();
//        for (int i = 0; i < 256; i++)
//            if (!bitmap[i])
//                temp.add(i);
//
//        int max = temp.size() + 1;
//        for (int i = 0; i < blockLen; i++) {
//            int block = (int) (0 + (Math.random() * (max - 0)));
//            temp.remove(block);
//            emptyBlocks[i] = block;
//            max--;
//        }
//        return emptyBlocks;
//    }
//
//    public void bitmapDeleteBlock (int blockNum) {
//        bitmap[blockNum] = false;
//    }
//
//    public void bitmapAllocBlock (int blockNum) {
//        bitmap[blockNum] = true;
//    }
//}

import java.util.BitSet;

public class Bitmap {
    private BitSet bitSet;
    private int size;

    public Bitmap(int size) {
        this.size = size; // Size in bits
        this.bitSet = new BitSet(size);
    }

    public void setUsed(int index) {
        bitSet.set(index);
    }

    public void setFree(int index) {
        bitSet.clear(index);
    }

    public boolean isUsed(int index) {
        return bitSet.get(index);
    }

    // prints the BitSet as a binary string
    public void print() {
        for (int i = 0; i < size; i++) {
            System.out.print(bitSet.get(i) ? "1" : "0");
        }
        System.out.println();
    }

    // Converts the BitSet to a byte array for storage
    public byte[] toByteArray() {
        byte[] bytes = new byte[(size + 7) / 8];
        for (int i = 0; i < size; i++) {
            if (bitSet.get(i)) {
                bytes[i / 8] |= 1 << (i % 8);
            }
        }
        return bytes;
    }
}
