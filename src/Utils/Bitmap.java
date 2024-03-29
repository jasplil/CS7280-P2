package Utils;

import java.util.ArrayList;

public class Bitmap {
    public boolean[] bitmap;

    Bitmap() {
        bitmap = new boolean[256];

        // empty disk
        for (int i = 0; i < 256; i++)
            bitmap[i] = false;
    }

    public int [] emptyBlocks(int blockLen) {
        int [] emptyBlocks = new int[blockLen];
        ArrayList<Integer> temp = new ArrayList<>();
        for (int i = 0; i < 256; i++)
            if (!bitmap[i])
                temp.add(i);

        int max = temp.size() + 1;
        for (int i = 0; i < blockLen; i++) {
            int block = (int) (0 + (Math.random() * (max - 0)));
            temp.remove(block);
            emptyBlocks[i] = block;
            max--;
        }
        return emptyBlocks;
    }

    public void bitmapDeleteBlock (int blockNum) {
        bitmap[blockNum] = false;
    }

    public void bitmapAllocBlock (int blockNum) {
        bitmap[blockNum] = true;
    }
}
