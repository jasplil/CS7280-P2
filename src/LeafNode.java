/**
 * Creating B+ Tree Index
 */
public class LeafNode extends Node {
    private LeafNode next;
    int maxNumPairs;
    int minNumPairs;
    int numPairs;
    LeafNode leftSibling;
    LeafNode rightSibling;
    DictionaryPair[] dictionary;

    public LeafNode(int m, DictionaryPair dp) { // dictionarypair node contains numvotes(key) and blocknumber(ptr)
        this.maxNumPairs = m - 1;
        this.minNumPairs = (int) (Math.ceil(m / 2) - 1);
        this.dictionary = new DictionaryPair[m]; // create instance of size m(max no of children)
        this.numPairs = 0;
        this.insert(dp);
        this.next = null;
    }

    public LeafNode(int m, DictionaryPair[] dps, InternalNode parent) { // if moved during insertion of new leafnode
        this.maxNumPairs = m - 1;
        this.minNumPairs = (int) (Math.ceil(m / 2) - 1);
        this.dictionary = dps;
        this.numPairs = linearNullSearch(dps);
        this.parent = parent;
        this.next = null;
    }

    public LeafNode getNext() {
        return next;
    }

    public void setNext(LeafNode n) {
        this.next = n;
    }

    private int linearNullSearch(DictionaryPair[] dps) { // linear search through all keys in node
        for (int i = 0; i < dps.length; i++) {
            if (dps[i] == null) {
                return i;
            }
        }
        return -1;
    }

    public int linearSearch(int key) { // linear search through all keys in node
        DictionaryPair[] dps = this.dictionary;
        for (int i = 0; i < dps.length; i++) {
            if (dps[i] == null) {
                continue;
            } else if (dps[i].key == key) {
                return i; // return index
            } else {
                continue;
            }
        }
        return -1;
    }

    public void printAllDictionaryPairs() {
        DictionaryPair[] dps = this.dictionary;
        for (int i = 0; i < dps.length; i++) {
            if (dps[i] != null) {
                System.out.println("key is: " + dps[i].key);
                System.out.println("value is: " + dps[i].value);
            }

        }
        System.out.println("----------");
    }

    public void printIndexNode(int key1, int key2) {
        DictionaryPair[] dps = this.dictionary;
        int count = 0;
        for (int i = 0; i < dps.length; i++) {
            if (dps[i] != null) {
                if (key1 < dps[i].key && dps[i].key < key2 || dps[i].key == key1 || dps[i].key == key2) {
                    count++;
                }
            }
        }
        for (int i = 0; i < dps.length; i++) {
            if (dps[i] != null) {
                if (count > 0) {
                    System.out.println("key is: " + dps[i].key);
                    System.out.println("value is: " + dps[i].value);
                }
            }
        }
        if (count > 0) {
            System.out.println("----------");
        }
    }

    public int getNoofIndexblocksaccessed(int key1, int key2) {
        DictionaryPair[] dps = this.dictionary;
        int count = 0;
        int indexblock = 0;
        for (int i = 0; i < dps.length; i++) {
            if (dps[i] != null) {
                if (key1 < dps[i].key && dps[i].key < key2 || dps[i].key == key1 || dps[i].key == key2) {
                    count++;
                }
            }
        }
        if (count > 0) {
            indexblock++;
        }
        return indexblock;
    }

    public void delete(int index) {
        this.dictionary[index] = null;
        numPairs--;
    }

    public boolean insert(DictionaryPair dp) {
        if (this.isFull()) {
            return false;
        } else {
            this.dictionary[numPairs] = dp;
            numPairs++;

            return true;
        }
    }

    public boolean isDeficient() {
        return numPairs < minNumPairs;
    }

    public boolean isFull() {
        return numPairs == maxNumPairs;
    }

    public boolean isLendable() {
        return numPairs > minNumPairs;
    }

    public boolean isMergeable() {
        return numPairs == minNumPairs;
    }

}