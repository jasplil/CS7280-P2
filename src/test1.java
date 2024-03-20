/**
 * Designing and implementing a database storage system of 100B
 * Experiment 1
 * @author: Bachhas Nikita
 * @author: Kam Chin Voon
 * */
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;
import java.io.FileWriter;
import java.lang.String;

public class test1 {

    public static void main(String[] arg) throws IOException {

        BufferedReader TSVFile = new BufferedReader(new FileReader("data.tsv"));

        String dataRow = TSVFile.readLine(); // Read first line.
        int remainingSpaces;
        int itemNumber;
        int startingByte;
        int totalBytes = 0;
        String[] blockArr = new String[] {};
        int countBlocks = 0;
        ArrayList<String> blockArray = new ArrayList<String>(Arrays.asList(blockArr));
        FileWriter writer = new FileWriter("output_100B.txt");
        FileWriter writer1 = new FileWriter("records_100B.txt");
        dataRow = TSVFile.readLine(); // Read next line of data.
        while (dataRow != null) {
            remainingSpaces = 60; // 100b - 40(for extra space) bytes
            blockArray.clear();
            itemNumber = 0;
            startingByte = 30; // For unique index number
            while (dataRow != null && remainingSpaces > dataRow.getBytes().length) {

                String[] dataArray = dataRow.split("\t");
                String id = dataArray[0];
                if (id.chars().count() > 9) {
                    id = id.substring(0, id.length() - 1);
                }

                for (String item : dataArray) {
                    blockArray.add(item);
                }
                blockArray.add(itemNumber, String.valueOf(startingByte));

                itemNumber++;
                startingByte += dataRow.getBytes().length;
                remainingSpaces -= dataRow.getBytes().length;
                dataRow = TSVFile.readLine(); // Read next line of data.

                if (itemNumber == 3) { // Setting max number of records in a datablock to be = 26 for easier
                    // referencing in experiment 3 and 4 partb (b)
                    writer1.write(id + " " + String.valueOf(countBlocks)); // write to file
                }
            }
            for (String str : blockArray) {
                writer.write(str + " "); // write to file
                totalBytes += str.getBytes().length + 1;
            }
            writer.write(System.lineSeparator());
            writer1.write(System.lineSeparator());

            countBlocks++;

        }
        // Close the file once all data has been read.
        TSVFile.close();
        writer.close();
        writer1.close();

        // Experiment 1 - Part (a): the number of blocks
        System.out.println("Total number of blocks is: " + countBlocks);

        // Experiment 1 - Part (b): - the size of database (in terms of MB)
        System.out.println("Total number of bytes(i.e. the size of the file) is (in Megabytes): "
                + totalBytes / 1024 / 1024 + " MB ");

        // End the printout with a blank line.
        System.out.println();

        System.out.println("Success");

        // Removing empty lines from the records file
        Scanner file;
        PrintWriter writer2;
        try {

            file = new Scanner(new File("records_100B.txt"));
            writer2 = new PrintWriter("recordsnoemptylines_100B.txt");

            while (file.hasNext()) {
                String line = file.nextLine();
                if (!line.isEmpty()) {
                    writer2.write(line);
                    writer2.write("\n");
                }
            }

            file.close();
            writer2.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        File file1 = new File("records_100B.txt");
        File file2 = new File("recordsnoemptylines_100B.txt");
        file1.delete();
        file2.renameTo(file1);

    } // main()

}