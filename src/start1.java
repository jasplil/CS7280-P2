/**
 * Running the B+ Tree Index with 100B database storage
 * Experiment 2
 * Experiment 3
 * Experiment 4
 * Experiment 5
 * @author: Bachhas Nikita
 * @author: Kam Chin Voon
 * */
/**
 * Running the B+ Tree Index with 500B database storage
 * Re-doing Experiment 2
 * Re-doing Experiment 3
 * Re-doing Experiment 4
 * Re-doing Experiment 5
 * @author: Bachhas Nikita
 * */
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class start1 {

    public static void main(String[] args) {
        String path = "data.tsv"; // creating a variable path and storing the filepath
        String line = ""; // variable to be used later on
        String path1 = "output_100B.txt";
        String line1 = "";
        String path2 = "records_100B.txt";
        String line2 = "";
        ArrayList<Integer> uniqueID = new ArrayList<Integer>(); // Create an ArrayList object
        ArrayList<Double> ratings = new ArrayList<Double>();
        ArrayList<Integer> blockid = new ArrayList<Integer>();
        ArrayList<String> blockidisstoredin = new ArrayList<String>();
        ArrayList<String> linetostorein = new ArrayList<String>();
        ArrayList<Integer> blockid3 = new ArrayList<Integer>();
        ArrayList<String> blockidisstoredin1 = new ArrayList<String>();
        ArrayList<String> linetostorein1 = new ArrayList<String>();
        // int num = 200; // number of records entering the B+ tree
        int key = 0;
        int iteration = 0; // so that we can skip the first line - i.e. header
        BTree bpt = new BTree(8); // 8 = max number of pointers in each node + hence, number of keys in the tree =
        // 8-1 = 6
        /*
         * Total Number of available bytes = 100 We assign each pointer to be 2 bytes
         * --> each block has m + 1 pointers Larget key = 2279223 --> 7 bytes Largest
         * values = 9916778 --> 7 bytes Hence each datablock can hold a n number of KV &
         * pointers (7+7+2=16 bytes) + an extra pointer (2 bytes) Thus, Each datablock
         * holds (100-2)/16 = 7 KV pairs
         */
        // 32-1 = 31
        /*
         * Total Number of available bytes = 500 We assign each pointer to be 2 bytes
         * --> each block has m + 1 pointers Larget key = 2279223 --> 7 bytes Largest
         * values = 9916778 --> 7 bytes Hence each datablock can hold a n number of KV
         * pairs & pointers (7+7+2=16 bytes) + an extra pointer (2 bytes) Thus, Each
         * datablock holds (500-2)/16 = 31 KV pairs
         */

        try {
            BufferedReader inputfile = new BufferedReader(new FileReader(path)); // read tsv file using a BufferedReader
            BufferedReader inputfile1 = new BufferedReader(new FileReader(path1)); // read txt file using a BufferedReader
            BufferedReader inputfile2 = new BufferedReader(new FileReader(path2)); // read txt file using a BufferedReader

            while ((line = inputfile.readLine()) != null) { // as long as there is values in the lines,
                // the while loop will continue running
                if (iteration == 0) { // so that we can skip the first line - i.e. header
                    iteration++;
                    continue;
                }

                String[] inputdata = line.split(("	")); // spliiting the columns

                String keys = inputdata[2]; // getting the keys = numvotes
                int i = Integer.parseInt(keys); // B+tree can only take in integers --> so we need to convert from string to int

                String values = inputdata[0]; // getting the values = unique id
                if (values.chars().count() > 9) {
                    values = values.substring(0, values.length() - 1);
                }
                int j = Integer.parseInt(values.substring(2)); // B+tree can only take in integers --> so we need to first
                // remove the first two letter from the id and then convert it from string to
                // int
                uniqueID.add(j);

                String AvgRatings = inputdata[1];
                double d = Double.parseDouble(AvgRatings);
                ratings.add(d);

                bpt.insert(i, j); // inserting each key-value pair into the B+tree
                iteration++;
                if (i > key) {
                    key = i;
                }
            }
            inputfile.close();

            System.out.println("");
            System.out.println("Loading...");

            // experiment 3 part (c)
            ArrayList<Double> movieid = new ArrayList<Double>(); // Create an ArrayList object
            movieid = bpt.search(500, 500);
            int a = 0;
            for (int i = 0; i < movieid.size(); i++) {
                double ID = movieid.get(i);
                int IV = (int) ID;
                int temp = 0;
                while ((line2 = inputfile2.readLine()) != null) {
                    String[] inputdata2 = line2.split((" ")); // spliiting the columns
                    String lastaccessedid = inputdata2[0];
                    int LA = Integer.parseInt(lastaccessedid.substring(2));
                    String blockstoredin = null;
                    if (LA == IV || temp < IV && IV < LA) {
                        blockstoredin = inputdata2[1];
                        if (a < 5) {
                            blockidisstoredin.add("This is the data block number: " + blockstoredin);
                        }
                        inputfile2 = new BufferedReader(new FileReader(path2));
                        int bsi = 0;
                        if (blockstoredin != null) {
                            bsi = Integer.parseInt(blockstoredin);
                            int count = 0;
                            for (int k = 0; k < blockid.size(); k++) {
                                if (blockid.get(k) == bsi) {
                                    count++;
                                }
                            }
                            if (count == 0) {
                                blockid.add(bsi);
                            }
                        }
                        int iteration1 = 0;
                        while ((line1 = inputfile1.readLine()) != null) {
                            if (iteration1 == bsi) {
                                if (a < 5) {
                                    linetostorein.add(line1);
                                    a++;
                                }
                                inputfile1 = new BufferedReader(new FileReader(path1));
                                break;
                            }
                            iteration1++;
                        }
                        break;
                    }
                    temp = LA; // temp holds the Last accessed id from the previous record
                }
            }

            // experiment 4 part (c)
            ArrayList<Double> movieid3 = new ArrayList<Double>(); // Create an ArrayList object
            movieid3 = bpt.search(30000, 400000);
            int a1 = 0;
            for (int i = 0; i < movieid3.size(); i++) {
                double ID = movieid3.get(i);
                int IV = (int) ID;
                int temp = 0;
                while ((line2 = inputfile2.readLine()) != null) {
                    String[] inputdata2 = line2.split((" ")); // spliiting the columns
                    String lastaccessedid = inputdata2[0];
                    int LA = Integer.parseInt(lastaccessedid.substring(2));
                    String blockstoredin1 = null;
                    if (LA == IV || (LA > IV && IV > temp)) {
                        blockstoredin1 = inputdata2[1];
                        if (a1 < 5) {
                            blockidisstoredin1.add("This is the data block number: " + blockstoredin1);
                        }
                        inputfile2 = new BufferedReader(new FileReader(path2));
                        int bsi = 0;
                        if (blockstoredin1 != null) {
                            bsi = Integer.parseInt(blockstoredin1);
                            int count = 0;
                            for (int k = 0; k < blockid3.size(); k++) {
                                if (blockid3.get(k) == bsi) {
                                    count++;
                                }
                            }
                            if (count == 0) {
                                blockid3.add(bsi);
                            }
                        }
                        int iteration1 = 0;
                        while ((line1 = inputfile1.readLine()) != null) {
                            if (iteration1 == bsi) {
                                if (a1 < 5) {
                                    linetostorein1.add(line1);
                                    a1++;
                                }
                                inputfile1 = new BufferedReader(new FileReader(path1));
                                break;
                            }
                            iteration1++;
                        }
                        break;
                    }
                    temp = LA; // temp holds the Last accessed id from the previous record
                }
            }

            inputfile1.close();
            inputfile2.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("");

        System.out.println("Experiment 2 - Part (a): the parameter n of the B+ tree");
        System.out.println("Parameter n of B+ Tree (i.e. the maximum number of keys per node is) is " + (bpt.m - 1));
        System.out.println("");

        System.out.println("Experiment 2 - Part (b): The number of nodes of the B+ tree");
        System.out.println("Number of Nodes in the B+ Tree is " + bpt.countNodes());
        System.out.println("");

        System.out.println("Experiment 2 - Part(c): The height of the B+ tree, i.e., the number of levels of the B+ tree");
        System.out.println("Height of the B+ Tree is " + bpt.getHeight());
        System.out.println("");

        System.out.println("Experiment 2 - Part (d): The content of the root node and its 1st child node");
        bpt.getContentOfRootNodeAndFirstChild();
        System.out.println("__________________________");
        System.out.println("");

        System.out.println("Experiment 3 - Retrieve those movies with the “numVotes” equal to 500");
        System.out.println("Movie Ids with numvotes = 500: ");
        ArrayList<Double> movieid1 = new ArrayList<Double>(); // Create an ArrayList object
        movieid1 = bpt.search(500, 500);
        for (int i = 0; i < movieid1.size(); i++) {
            double id = movieid1.get(i);
            String s = String.valueOf((int) id);
            if (s.chars().count() == 7) {
                System.out.print("tt" + (int) id);
            } else if (s.chars().count() == 6) {
                System.out.print("tt0" + (int) id);
            } else if (s.chars().count() == 5) {
                System.out.print("tt00" + (int) id);
            } else if (s.chars().count() == 4) {
                System.out.print("tt000" + (int) id);
            } else if (s.chars().count() == 3) {
                System.out.print("tt0000" + (int) id);
            } else if (s.chars().count() == 2) {
                System.out.print("tt00000" + (int) id);
            } else if (s.chars().count() == 1) {
                System.out.print("tt000000" + (int) id);
            }
            System.out.print(", ");
        }
        System.out.println("");

        System.out.println("Experiment 3 - Part (a): The number and the contents of index nodes the process accesses");
        System.out.println("The contents of the first 5 index nodes are:");
        bpt.traversal(500, 500);
        System.out.println("");

        System.out.println("Experiment 3 - Part (b): The number and the content of data blocks the process accesses");
        System.out.println("The content of the top 5 data blocks are: ");
        System.out.println("__________________________");
        for (int i = 0; i < 5; i++) {
            System.out.println(blockidisstoredin.get(i));
            System.out.println(linetostorein.get(i));
        }
        System.out.println("Number of Data Blocks accessed is " + blockid.size());
        System.out.println("");

        System.out.println("Experiment 3 - Part (c): The average of “averageRating’s” of the records that are returned");
        Double average = 0.00;
        Double count = 0.00;
        for (int i = 0; i < movieid1.size(); i++) {
            double id = movieid1.get(i);
            int IntValue = (int) id;
            for (int k = 0; k < (iteration - 2); k++) {
                if (uniqueID.get(k) == IntValue) {
                    average = average + ratings.get(k);
                    count++;
                }
            }
        }
        average = average / count;
        System.out.println("Average of averageRating is: " + average);
        System.out.println("");

        System.out.println("Experiment 4: retrieve those movies with the attribute “numVotes” from 30,000 to 40,000");
        System.out.println("Movie Ids with numvotes = 30,000 and numvotes = 40,000: ");
        ArrayList<Double> movieid2 = new ArrayList<Double>(); // Create an ArrayList object
        movieid2 = bpt.search(30000, 40000);
        for (int i = 0; i < movieid2.size(); i++) {
            double id = movieid2.get(i);
            String s = String.valueOf((int) id);
            if (s.chars().count() == 7) {
                System.out.print("tt" + (int) id);
            } else if (s.chars().count() == 6) {
                System.out.print("tt0" + (int) id);
            } else if (s.chars().count() == 5) {
                System.out.print("tt00" + (int) id);
            } else if (s.chars().count() == 4) {
                System.out.print("tt000" + (int) id);
            } else if (s.chars().count() == 3) {
                System.out.print("tt0000" + (int) id);
            } else if (s.chars().count() == 2) {
                System.out.print("tt00000" + (int) id);
            } else if (s.chars().count() == 1) {
                System.out.print("tt000000" + (int) id);
            }
            System.out.print(", ");
        }
        System.out.println("");

        System.out.println("Experiment 4: Part (a): The number and the content of index nodes the process accesses");
        System.out.println("The contents of the first 5 index nodes are:");
        bpt.traversal(30000, 40000);
        System.out.println("");

        System.out.println("Experiment 4 - Part (b): The number and the content of data blocks the process accesses");
        System.out.println("The content of the top 5 data blocks are: ");
        System.out.println("__________________________");
        for (int i = 0; i < 5; i++) {
            System.out.println(blockidisstoredin1.get(i));
            System.out.println(linetostorein1.get(i));
        }
        System.out.println("Number of Data Blocks accessed is " + blockid3.size());
        System.out.println("");

        System.out.println("Experiment 4 - Part (c):The average of “averageRating’s” of the records that are returned");
        Double average1 = 0.00;
        Double count1 = 0.00;
        for (int i = 0; i < movieid2.size(); i++) {
            double id = movieid2.get(i);
            int IntValue = (int) id;
            for (int k = 0; k < (iteration - 2); k++) {
                if (uniqueID.get(k) == IntValue) {
                    average1 = average1 + ratings.get(k);
                    count1++;
                }
            }
        }
        average1 = average1 / count1;
        System.out.println("Average of averageRating is: " + average1);
        System.out.println("");

        System.out.println("Experiment 5: delete those movies with the attribute “numVotes” equal to 1,000");
        int originalNodeCount = bpt.nodeCount;
        boolean t = true;
        while (t) {
            t = bpt.delete(1000);
        }
        int nodesAfterDeletion = bpt.nodeCount;
        System.out.println("");

        System.out.println(
                "Experiment 5 - Part (a): The number of times that a node is deleted (or two nodes are merged) during the process of the updating the B+ tree");
        System.out.println("Number of nodes deleted/merged is: ");
        System.out.println(originalNodeCount - nodesAfterDeletion);
        System.out.println("");

        System.out.println("Experiment 5 - Part (b): The number nodes of the updated B+ tree");
        System.out.println("Number of Nodes in the B+ Tree is " + bpt.countNodes());
        System.out.println("");

        System.out.println("Experiment 5 - Part (C):The height of the updated B+ tree");
        System.out.println("Height after deletion is: ");
        System.out.println(bpt.height);
        System.out.println("");

        System.out
                .println("Experiment 5 - Part (D):The content of the root node and its 1st child node of the updated B+ tree");
        bpt.getContentOfRootNodeAndFirstChild();
        System.out.println("");
    }

}