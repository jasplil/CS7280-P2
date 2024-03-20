/**
 * Creating B+ Tree Index
 * @author: Kam Chin Voon
 */
public class InternalNode extends Node { // all nodes that are not leaf nodes
    int maxDegree; // max children -> m
    int minDegree; // min children -> (m)/2
    int degree; // keep track of no of children currently
    InternalNode leftSibling;
    InternalNode rightSibling;
    Integer[] keys;
    Node[] childPointers;

    public InternalNode(int m, Integer[] keys) {
        this.maxDegree = m;
        this.minDegree = (int) Math.ceil(m / 2.0);
        this.degree = 0;
        this.keys = keys;
        this.childPointers = new Node[this.maxDegree + 1];
    }

    public InternalNode(int m, Integer[] keys, Node[] pointers) {
        this.maxDegree = m;
        this.minDegree = (int) Math.ceil(m / 2.0);
        this.degree = linearNullSearch(pointers);
        this.keys = keys;
        this.childPointers = pointers;
    }

    private int linearNullSearch(Node[] pointers) {
        for (int i = 0; i < pointers.length; i++) {
            if (pointers[i] == null) {
                return i;
            }
        }
        return -1;
    }

    public void appendChildPointer(Node pointer) {
        this.childPointers[degree] = pointer;
        this.degree++;
    }

    public int findIndexOfPointer(Node pointer) {
        for (int i = 0; i < childPointers.length; i++) {
            if (childPointers[i] == pointer) {
                return i;
            }
        }
        return -1;
    }

    public void insertChildPointer(Node pointer, int index) {
        for (int i = degree - 1; i >= index; i--) {
            childPointers[i + 1] = childPointers[i];
        }
        this.childPointers[index] = pointer;
        this.degree++;
    }

    public boolean isDeficient() {
        return this.degree < this.minDegree;
    }

    public boolean isLendable() {
        return this.degree > this.minDegree;
    }

    public boolean isMergeable() {
        return this.degree == this.minDegree;
    }

    public boolean isOverfull() {
        return this.degree == maxDegree + 1;
    }

    public void prependChildPointer(Node pointer) {
        for (int i = degree - 1; i >= 0; i--) {
            childPointers[i + 1] = childPointers[i];
        }
        this.childPointers[0] = pointer;
        this.degree++;
    }

    public void removeKey(int index) {
        this.keys[index] = null;
    }

    public void removePointer(int index) {
        this.childPointers[index] = null;
        this.degree--;
    }

    public void removePointer(Node pointer) {
        for (int i = 0; i < childPointers.length; i++) {
            if (childPointers[i] == pointer) {
                this.childPointers[i] = null;
            }
        }
        this.degree--;
    }

    public int getDegree() {
        return this.degree;
    }

    public void sortKeys() {
        Integer[] new_keys = this.keys;
        int j = 0;
        for (int i = 0; i < this.keys.length; i++) {
            if (this.keys[i] == null && i + 1 != this.keys.length) {
                i++;
                new_keys[j] = this.keys[i];
                j++;
            } else {
                new_keys[j] = this.keys[i];
                j++;
            }
        }
        new_keys[degree - 1] = null;
        this.keys = new_keys;
    }
}