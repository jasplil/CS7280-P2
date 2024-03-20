/**
 * Running the B+ Tree Index with 100B database storage
 * */
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;

// the number of times that a node is deleted (or two nodes are merged)
// during the process of the updating the B+ tree
// - the number nodes of the updated B+ tree ||done
// - the height of the updated B+ tree ||done
// - the content of the root node and its 1st child node of the updated B+ ||done
// tree 

public class BTree {
    int m;
    InternalNode root;
    LeafNode firstLeaf;
    int height;
    int nodeCount;

    ArrayList<Integer> experimentIndexCount = new ArrayList<>();
    ArrayList<Integer> experimentDataCount = new ArrayList<>();

    public BTree(int m) {
        this.m = m; // max no of pointers
        this.root = null; // nothing at root yet
        this.height = 0;
        this.nodeCount = 0;

    }

    // Experiment 2 - Part (d): The content of the root node and its 1st child node
    public void getContentOfRootNodeAndFirstChild() {
        Integer[] keys = root.keys;
        for (int i = 0; i < keys.length; i++) {
            if (keys[i] != null) {
                System.out.println("Root node key is: " + keys[i]);
            }
        }
        if (this.root.childPointers[0] instanceof InternalNode) {
            InternalNode child = (InternalNode) this.root.childPointers[0];
            if (child != null) {
                for (int i = 0; i < this.m; i++) {
                    if (child.keys[i] != null) {
                        System.out.println("First child contains: " + child.keys[i]);
                    }
                }
            }
        } else {
            LeafNode child = (LeafNode) this.root.childPointers[0];
            if (child != null) {
                for (int i = 0; i < this.m; i++) {
                    if (child.dictionary[i] != null) {
                        System.out.println("First child contains: " + child.dictionary[i].key);
                    }
                }
            }
        }
    }

    private int binarySearch(DictionaryPair[] dps, int numPairs, int t) {
        Comparator<DictionaryPair> c = new Comparator<DictionaryPair>() {
            @Override
            public int compare(DictionaryPair o1, DictionaryPair o2) {
                Integer a = Integer.valueOf(o1.key);
                Integer b = Integer.valueOf(o2.key);
                return a.compareTo(b);
            }
        };
        return Arrays.binarySearch(dps, 0, numPairs, new DictionaryPair(t, 0), c);
    }

    public int getHeight() {
        return this.height;
    }

    // Experiment 3 - Retrieve those movies with the “numVotes” equal to 500 & Part
    // (a): The number and the contents of index nodes the process accesses
    public Double search(int key) {

        LeafNode ln = (this.root == null) ? this.firstLeaf : findLeafNode(key);

        DictionaryPair[] dps = ln.dictionary;

        if (isAllNull(dps) == false) { // there are values

            int index = binarySearch(dps, ln.numPairs, key);

            if (index < 0) {
                System.out.println("Search successful: No results were found ");
                return null;
            } else {
                System.out.println("Search successful: This is the result found: ");
                return dps[index].value;
            }
        }
        return null;
    }

    // Experiment 4: retrieve those movies with the attribute “numVotes” from 30,000
    // to 40,000 & Part (a): The number and the content of index nodes the process
    // accesses
    public ArrayList<Double> search(int lowerBound, int upperBound) {
        this.experimentDataCount.clear();
        this.experimentIndexCount.clear();
        getFirstKey(this.root);
        ArrayList<Double> values = new ArrayList<Double>();

        LeafNode currNode = this.firstLeaf;
        while (currNode != null) {

            DictionaryPair dps[] = currNode.dictionary;
            for (DictionaryPair dp : dps) {

                if (dp == null) {
                    break;
                }

                if (lowerBound <= dp.key && dp.key <= upperBound) {
                    experimentDataCount.add(dp.key);
                    values.add(dp.value);
                }
            }
            currNode = currNode.rightSibling;

        }

        return values;
    }

    private boolean isEmpty() {
        if (this.firstLeaf == null) {
            return true;
        }
        return false;

    }

    private boolean isAllNull(DictionaryPair[] dps) {

        int j = 0;
        for (int i = 0; i < dps.length; i++) {
            if (dps[i] == null) {
                j++;
            }
        }
        if (j == this.m) { // true if all values are null
            System.out.println("j is bjxbs");
            return true;
        }
        return false; // false if there are values
    }

    // Balance the tree
    public int handleDeficiency(InternalNode in, int countMergeDelete) {

        InternalNode sibling;
        InternalNode parent = in.parent;

        if (this.root == in) {
            for (int i = 0; i < in.childPointers.length; i++) {
                if (in.childPointers[i] != null) {
                    if (in.childPointers[i] instanceof InternalNode) {
                        this.root = (InternalNode) in.childPointers[i];
                        this.root.parent = null;
                    } else if (in.childPointers[i] instanceof LeafNode) {
                        this.root = null;
                    }
                }
            }
        }

        else if (in.leftSibling != null && in.leftSibling.isLendable()) {
            countMergeDelete++;
            sibling = in.leftSibling;
        } else if (in.rightSibling != null && in.rightSibling.isLendable()) {
            countMergeDelete++;
            sibling = in.rightSibling;

            int borrowedKey = sibling.keys[0];
            Node pointer = sibling.childPointers[0];

            in.keys[in.degree - 1] = parent.keys[0];
            in.childPointers[in.degree] = pointer;

            parent.keys[0] = borrowedKey;

            sibling.removePointer(0);
            Arrays.sort(sibling.keys);
            sibling.removePointer(0);
            shiftDown(in.childPointers, 1);
        } else if (in.leftSibling != null && in.leftSibling.isMergeable()) {
            countMergeDelete++;
        } else if (in.rightSibling != null && in.rightSibling.isMergeable()) {
            countMergeDelete++;
            sibling = in.rightSibling;
            sibling.keys[sibling.degree - 1] = parent.keys[parent.degree - 2];
            Arrays.sort(sibling.keys, 0, sibling.degree);
            parent.keys[parent.degree - 2] = null;

            for (int i = 0; i < in.childPointers.length; i++) {
                if (in.childPointers[i] != null) {
                    sibling.prependChildPointer(in.childPointers[i]);
                    in.childPointers[i].parent = sibling;
                    in.removePointer(i);
                }
            }

            parent.removePointer(in);

            sibling.leftSibling = in.leftSibling;
        }

        if (parent != null && parent.isDeficient()) {
            handleDeficiency(parent, countMergeDelete);
        }
        System.out.println("tree balanced");
        return countMergeDelete;

    }

    private void shiftDown(Node[] pointers, int amount) {
        Node[] newPointers = new Node[this.m + 1];
        for (int i = amount; i < pointers.length; i++) {
            newPointers[i - amount] = pointers[i];
        }
        pointers = newPointers;
    }

    public void insert(int key, double value) {
        if (isEmpty()) {

            LeafNode ln = new LeafNode(this.m, new DictionaryPair(key, value));

            this.firstLeaf = ln;
            this.height++;
            this.nodeCount++;

        } else {

            LeafNode ln = (this.root == null) ? this.firstLeaf : findLeafNode(key);

            if (!ln.insert(new DictionaryPair(key, value))) { // if leafnode is full
                ln.dictionary[ln.numPairs] = new DictionaryPair(key, value);
                ln.numPairs++;
                sortDictionary(ln.dictionary);

                int midpoint = getMidpoint();
                DictionaryPair[] halfDict = splitDictionary(ln, midpoint); // half dictionary is the back half of the split

                if (ln.parent == null) { // if leaf node does not have parent
                    System.out.println("else");
                    Integer[] parent_keys = new Integer[this.m];
                    parent_keys[0] = halfDict[0].key; // first key in back half of dictionary becomes parent
                    InternalNode parent = new InternalNode(this.m, parent_keys);
                    ln.parent = parent;
                    parent.appendChildPointer(ln);
                    this.height++;
                    this.nodeCount++;

                } else { // add to parent node
                    int newParentKey = halfDict[0].key;
                    ln.parent.keys[ln.parent.degree - 1] = newParentKey;
                    Arrays.sort(ln.parent.keys, 0, ln.parent.degree);
                }

                LeafNode newLeafNode = new LeafNode(this.m, halfDict, ln.parent);
                this.nodeCount++;

                int pointerIndex = ln.parent.findIndexOfPointer(ln) + 1;
                ln.parent.insertChildPointer(newLeafNode, pointerIndex);

                newLeafNode.rightSibling = ln.rightSibling;
                if (newLeafNode.rightSibling != null) {
                    newLeafNode.rightSibling.leftSibling = newLeafNode;
                }
                ln.rightSibling = newLeafNode;
                newLeafNode.leftSibling = ln;

                ln.setNext(newLeafNode); // point to next leafnode
                if (newLeafNode.parent.rightSibling != null) {
                    LeafNode siblingLeaf = (LeafNode) newLeafNode.parent.rightSibling.childPointers[0];
                    newLeafNode.setNext(siblingLeaf);
                }

                // newLeafNode.setNext(newLeafNode.leftSibling);

                if (this.root == null) {

                    this.root = ln.parent;

                } else {
                    InternalNode in = ln.parent;
                    while (in != null) {
                        if (in.isOverfull()) {
                            splitInternalNode(in);
                            this.height++;
                        } else {
                            break;
                        }
                        in = in.parent;
                    }
                }
            }
        }
        System.out.println("inserted " + key + " and " + value);
    }

    private void sortDictionary(DictionaryPair[] dictionary) {
        Arrays.sort(dictionary, new Comparator<DictionaryPair>() {
            @Override
            public int compare(DictionaryPair o1, DictionaryPair o2) {
                if (o1 == null && o2 == null) {
                    return 0;
                }
                if (o1 == null) {
                    return 1;
                }
                if (o2 == null) {
                    return -1;
                }
                return o1.compareTo(o2);
            }
        });
    }

    private DictionaryPair[] splitDictionary(LeafNode ln, int split) {

        DictionaryPair[] dictionary = ln.dictionary;

        DictionaryPair[] halfDict = new DictionaryPair[this.m];

        for (int i = split; i < dictionary.length; i++) {
            halfDict[i - split] = dictionary[i];
            ln.delete(i);
        }

        return halfDict;
    }

    private void splitInternalNode(InternalNode in) { // split when inserted node > size m

        InternalNode parent = in.parent;

        int midpoint = getMidpoint();
        int newParentKey = in.keys[midpoint];
        Integer[] halfKeys = splitKeys(in.keys, midpoint);
        Node[] halfPointers = splitChildPointers(in, midpoint);

        InternalNode sibling = new InternalNode(this.m, halfKeys, halfPointers);
        this.nodeCount++;
        for (Node pointer : halfPointers) {
            if (pointer != null) {
                pointer.parent = sibling;
            }
        }

        sibling.rightSibling = in.rightSibling;
        if (sibling.rightSibling != null) {
            sibling.rightSibling.leftSibling = sibling;
        }
        in.rightSibling = sibling;
        sibling.leftSibling = in;
        if (parent == null) {

            Integer[] keys = new Integer[this.m];
            keys[0] = newParentKey;
            InternalNode newRoot = new InternalNode(this.m, keys); // creating new parent --> height of B+ tree increases by 1
            newRoot.appendChildPointer(in);
            newRoot.appendChildPointer(sibling);
            this.root = newRoot;

            in.parent = newRoot;
            sibling.parent = newRoot;
            this.height++;
            this.nodeCount++;

        } else {

            parent.keys[parent.degree - 1] = newParentKey;
            Arrays.sort(parent.keys, 0, parent.degree);

            int pointerIndex = parent.findIndexOfPointer(in) + 1;
            parent.insertChildPointer(sibling, pointerIndex);
            sibling.parent = parent;
        }
    }

    private int getMidpoint() {
        return (int) Math.ceil((this.m + 1) / 2.0) - 1;
    }

    private Integer[] splitKeys(Integer[] keys, int split) {

        Integer[] halfKeys = new Integer[this.m];

        keys[split] = null;

        for (int i = split + 1; i < keys.length; i++) {
            halfKeys[i - split - 1] = keys[i];
            keys[i] = null;
        }

        return halfKeys;
    }

    private Node[] splitChildPointers(InternalNode in, int split) {

        Node[] pointers = in.childPointers;
        Node[] halfPointers = new Node[this.m + 1];

        for (int i = split + 1; i < pointers.length; i++) {
            halfPointers[i - split - 1] = pointers[i];
            in.removePointer(i);
        }

        return halfPointers;
    }

    // Find the leaf node
    private LeafNode findLeafNode(int key) {

        Integer[] keys = this.root.keys;
        int i;

        for (i = 0; i < this.root.degree - 1; i++) {
            if (key <= keys[i]) {
                break;
            }
        }

        Node child = this.root.childPointers[i];
        if (child instanceof LeafNode) {
            return (LeafNode) child;
        } else if (child != null) {
            return findLeafNode((InternalNode) child, key);
        } else {
            return null;
        }
    }

    // Find the leaf node
    private LeafNode findLeafNode(InternalNode node, int key) {

        if (node != null) {
            Integer[] keys = node.keys;
            int i;

            for (i = 0; i < node.degree - 1; i++) { // find index to stop at and look at child pointer
                if (keys[i] == null) {
                    return null;
                }
                if (key < keys[i]) {
                    break;
                }
            }
            Node childNode = node.childPointers[i];
            if (childNode instanceof LeafNode) {
                return (LeafNode) childNode;
            } else if (childNode instanceof InternalNode) {
                return findLeafNode((InternalNode) node.childPointers[i], key);
            } else {
                return null;
            }

        }
        return null;
    }

    // Finding the index of the pointer
    public int findIndexOfPointer(Node[] pointers, LeafNode node) {
        int i;
        for (i = 0; i < pointers.length; i++) {
            if (pointers[i] == node) {
                break;
            }
        }
        return i;
    }

    // Balance the tree
    public void balanceTree(Node child) { // child is the node that is deficient

        int borrowedKey; // key that we are going to borrow from the sibling node
        InternalNode parent = child.parent;
        Node pointer;

        if (child instanceof InternalNode) {

            InternalNode siblingBorrowedFrom; // option 1: borrowing keys, if sibling node has sufficient keys
            InternalNode siblingMergingWith; // options 2: merging with sibling node, if sibling node has insufficient keys
            InternalNode childIN = (InternalNode) child;

            if (this.root == child) { // if child is the root node
                for (int i = 0; i < childIN.childPointers.length; i++) {
                    if (childIN.childPointers[i] != null) { // get children nodes that have values
                        if (childIN.childPointers[i] instanceof InternalNode) { // if they are internal nodes
                            this.root = (InternalNode) childIN.childPointers[i]; // make them a parent
                            this.root.parent = null;
                            this.height--;
                            this.nodeCount--;
                        } else if (childIN.childPointers[i] instanceof LeafNode) {
                            this.root = null;
                        }
                    }
                }
            } else { // child is just a normal internal node
                if (childIN.leftSibling != null && childIN.leftSibling.isLendable()) { // isLendable() checks whether sibling
                    // nodes have sufficient keys or not

                } else if (childIN.rightSibling != null && childIN.rightSibling.isLendable()) {
                    System.out.println("lending from right sibling");

                    siblingBorrowedFrom = childIN.rightSibling;

                    borrowedKey = siblingBorrowedFrom.keys[0]; // get first key from right sibling
                    pointer = siblingBorrowedFrom.childPointers[0]; // get first pointer from right sibling

                    // Copy root key and pointer into parent
                    childIN.keys[childIN.degree - 1] = parent.keys[0];
                    childIN.childPointers[childIN.degree] = pointer;

                    // Copy borrowedKey into root
                    parent.keys[0] = borrowedKey;

                    // Delete key and pointer from sibling
                    siblingBorrowedFrom.removePointer(0);
                    siblingBorrowedFrom.sortKeys();
                    siblingBorrowedFrom.removePointer(0);
                    shiftDown(childIN.childPointers, 1);

                } else if (childIN.leftSibling != null && childIN.leftSibling.isMergeable()) { // isMergable checks whether
                    // sibling nodes + this.node,
                    // when merged will have
                    // sufficient keys or not

                    this.nodeCount--; // if yes, merged, then number of nodes decreases

                } else if (childIN.rightSibling != null && childIN.rightSibling.isMergeable()) {

                    siblingMergingWith = childIN.rightSibling;
                    this.nodeCount--;

                    siblingMergingWith.keys[siblingMergingWith.degree - 1] = parent.keys[parent.degree - 2];
                    Arrays.sort(siblingMergingWith.keys, 0, siblingMergingWith.degree);
                    parent.keys[parent.degree - 2] = null;

                    // Copy in's child pointer over to sibling's list of child pointers
                    for (int i = 0; i < childIN.childPointers.length; i++) {
                        if (childIN.childPointers[i] != null) {
                            siblingMergingWith.prependChildPointer(childIN.childPointers[i]);
                            childIN.childPointers[i].parent = siblingMergingWith;
                            childIN.removePointer(i);
                        }
                    }

                    // Delete child pointer from grandparent to deficient node
                    parent.removePointer(childIN);

                    // Remove left sibling
                    siblingMergingWith.leftSibling = childIN.leftSibling;
                }
                // Handle deficiency a level up if it exists
                if (parent != null && parent.isDeficient()) {
                    balanceTree(parent);
                }
            }

        } else if (child instanceof LeafNode) {

            LeafNode childLN = (LeafNode) child;
            LeafNode siblingBorrowedFrom;
            LeafNode siblingMergingWith;
            parent = childLN.parent;

            if (childLN.leftSibling != null // look at left sibling first
                    && childLN.leftSibling.parent == childLN.parent // they have the same parent
                    && childLN.leftSibling.isLendable()) {

                siblingBorrowedFrom = childLN.leftSibling;
                DictionaryPair borrowedDP = siblingBorrowedFrom.dictionary[siblingBorrowedFrom.numPairs - 1]; // only care about
                // dp

                // update keys in childln and sibling
                childLN.insert(borrowedDP); // add dp to leafnode that is deficient
                sortDictionary(childLN.dictionary);
                siblingBorrowedFrom.delete(siblingBorrowedFrom.numPairs - 1); // delete last dp from sibling that is borrowed

                // Update key in parent
                int pointerIndex = findIndexOfPointer(parent.childPointers, childLN); // get child pointer index of ln

                if (!(borrowedDP.key >= parent.keys[pointerIndex - 1])) {
                    parent.keys[pointerIndex - 1] = childLN.dictionary[0].key;
                }

            } else if (childLN.rightSibling != null && childLN.rightSibling.parent == childLN.parent
                    && childLN.rightSibling.isLendable()) {
                siblingBorrowedFrom = childLN.rightSibling;
                DictionaryPair borrowedDP = siblingBorrowedFrom.dictionary[0]; // borrow from the front of sibling

                // update keys in childln and sibling
                childLN.insert(borrowedDP);
                siblingBorrowedFrom.delete(0);
                sortDictionary(siblingBorrowedFrom.dictionary);

                // update key in parent
                int pointerIndex = findIndexOfPointer(parent.childPointers, childLN);

                if (!(borrowedDP.key < parent.keys[pointerIndex])) {
                    parent.keys[pointerIndex] = siblingBorrowedFrom.dictionary[0].key;
                }

            } else if (childLN.leftSibling != null && childLN.leftSibling.parent == childLN.parent
                    && childLN.leftSibling.isMergeable()) { // if cannot lend // check merge

                this.nodeCount--;
                siblingMergingWith = childLN.leftSibling;
                int pointerIndex = findIndexOfPointer(parent.childPointers, childLN);
                // add all terms inside left sibling

                // Remove key and child pointer from parent
                parent.removeKey(pointerIndex - 1);
                parent.removePointer(childLN);

                siblingMergingWith.rightSibling = childLN.rightSibling;
                if (parent.isDeficient()) {
                    balanceTree(parent);
                }

            } else if (childLN.rightSibling != null && childLN.rightSibling.parent == childLN.parent
                    && childLN.rightSibling.isMergeable()) {

                this.nodeCount--;
                siblingMergingWith = childLN.rightSibling;
                int pointerIndex = findIndexOfPointer(parent.childPointers, childLN);

                // Remove key and child pointer from parent
                parent.removeKey(pointerIndex);
                parent.removePointer(pointerIndex);

                siblingMergingWith.leftSibling = childLN.leftSibling;

                if (siblingMergingWith.leftSibling == null) {
                    this.firstLeaf = siblingMergingWith;
                }

                if (parent.isDeficient()) {
                    balanceTree(parent);
                }

            }
        } else {
            System.out.println("failure to balance");
        }

        // Handle deficiency a level up if it exists
        if (parent != null && parent.isDeficient()) {
            balanceTree(parent);
        }
    }

    public boolean delete(int key) {
        LeafNode first;
        int index;
        if (isEmpty()) { // empty tree

            System.out.println("Empty tree. None deleted.");
            return false;

        } else if (Search(key) == null) { // key not in tree
            System.out.println("Could not find key. None deleted.");
            return false;
        } else { // key in tree

            // Get leaf node and attempt to find index of key to delete
            first = (this.root == null) ? this.firstLeaf : findLeafNode(key);
            if (first != null) {
                index = first.linearSearch(key);
                if (index == -1) { // not in leafnode
                    return false;
                } else {
                    first.delete(index);
                    if (first.isDeficient()) {
                        balanceTree(first);
                    } else if (this.root == null && this.firstLeaf.numPairs == 0) {

                        this.firstLeaf = null;

                    } else {
                        sortDictionary(first.dictionary);

                    }
                }
            } else {
                return false;
            }

        }
        return true;
    }

    public ArrayList<Integer> Search(int key) {

        ArrayList<Integer> values = new ArrayList<Integer>();

        LeafNode currNode = this.firstLeaf;
        while (currNode != null) {

            DictionaryPair dps[] = currNode.dictionary;
            for (DictionaryPair dp : dps) {

                if (dp == null) {
                    break;
                }

                if (key <= dp.key && dp.key <= key) {
                    values.add(dp.key);
                }
            }
            currNode = currNode.rightSibling;

        }

        return values;
    }

    public void traversal(int key1, int key2) {
        int NoofIndexblocks = 0;
        InternalNode childIN;
        Node child = this.root.childPointers[0];
        if (child instanceof LeafNode) { // only 1 level
            LeafNode childLN = (LeafNode) child;
            childLN.printIndexNode(key1, key2);
            NoofIndexblocks = NoofIndexblocks + childLN.getNoofIndexblocksaccessed(key1, key2);
            while (childLN.leftSibling != null) {
                childLN = childLN.leftSibling;
                NoofIndexblocks = NoofIndexblocks + childLN.getNoofIndexblocksaccessed(key1, key2);
                if (NoofIndexblocks < 5) {
                    childLN.printIndexNode(key1, key2);
                }
            }
            while (childLN.rightSibling != null) {
                childLN = childLN.rightSibling;
                NoofIndexblocks = NoofIndexblocks + childLN.getNoofIndexblocksaccessed(key1, key2);
                if (NoofIndexblocks < 5) {
                    childLN.printIndexNode(key1, key2);
                }
            }
        } else if (child != null) { // many levels
            while (!(child instanceof LeafNode) && child != null) { // traverse to the bottom
                childIN = (InternalNode) child;
                child = childIN.childPointers[0];
            }
            if (child == null) {
                System.out.println("error");
            } else { // if at leaf node
                LeafNode childLN = (LeafNode) child;
                childLN.printIndexNode(key1, key2);
                while (childLN.leftSibling != null) {
                    childLN = childLN.leftSibling;
                    NoofIndexblocks = NoofIndexblocks + childLN.getNoofIndexblocksaccessed(key1, key2);
                    if (NoofIndexblocks < 5) {
                        childLN.printIndexNode(key1, key2);
                    }
                }
                while (childLN.rightSibling != null) {
                    childLN = childLN.rightSibling;
                    NoofIndexblocks = NoofIndexblocks + childLN.getNoofIndexblocksaccessed(key1, key2);
                    if (NoofIndexblocks < 5) {
                        childLN.printIndexNode(key1, key2);
                    }
                }
            }
        } else {
            System.out.println("empty tree");
        }
        System.out.println("The total number of index nodes accessed is: " + NoofIndexblocks);
    }

    private int getFirstKey(Node node) { // recursively gets the first leaf node first dict pair
        if (node instanceof LeafNode) {

            LeafNode ln = (LeafNode) node;
            this.experimentDataCount.add(ln.dictionary[0].key);
            return ln.dictionary[0].key;
        } else if (node instanceof InternalNode) {
            InternalNode in = (InternalNode) node;
            this.experimentIndexCount.add(in.keys[0]);
            if (in == this.root) {
                return this.firstLeaf.dictionary[0].key;
            }

            return getFirstKey(in.childPointers[0]);
        } else {
            return -1;
        }
    }

    public int countNodes() {
        return this.nodeCount;
    }
}