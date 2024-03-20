/**
 * Creating B+ Tree Index
 * @author: Kam Chin Voon
 */
public class DictionaryPair implements Comparable<DictionaryPair> {
    int key; // numVotes
    double value; // address: blockNumber

    public DictionaryPair(int key, double value) {
        this.key = key;
        this.value = value;
    }

    public int compareTo(DictionaryPair o) {
        if (key == o.key) { // same values
            return 0;
        } else if (key > o.key) { // current value greater than o
            return 1;
        } else { // numVotes smaller than other blocknumber's numVotes
            return -1;
        }
    }

    public int getNumVotes() {
        return key;
    }

    public double getBlockNumber() {
        return value;
    }
}