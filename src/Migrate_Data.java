import java.io.FileNotFoundException;
import java.io.File;
import java.util.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;


public class Migrate_Data {
    private static int Create_Database(Connection con){
        /**
         * Only Create Table if First time running program
         * If Database was set up before hand
         */
        try{
            String sql_table = "CREATE TABLE IF NOT EXISTS Fields (\n"
                    + " A TEXT,\n"
                    + " B TEXT,\n"
                    + " C TEXT,\n"
                    + " D TEXT,\n"
                    + " E TEXT,\n"
                    + " F TEXT,\n"
                    + " G TEXT,\n"
                    + " H TEXT,\n"
                    + " I TEXT,\n"
                    + " J TEXT\n"
                    + ");";
            Statement stmt = con.createStatement();
            stmt.execute(sql_table);

        } catch (SQLException e) {
            System.out.println(e.getMessage());
            return 0;
        }
        return 1;
    }
    public static void main(String[] args) throws FileNotFoundException {
        /**Create and Establish Connection to DB
         * Make Bad Data File
         * Establish Records
         * Uses sqlite-jdbc-3.23.1.jar as driver, must be added to classpath
         **/
        String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date());
        PrintWriter pw = new PrintWriter(new File("bad-data-<"+timeStamp+">.csv"));
        Connection con = null;
        int records_recieved=0;
        int records_succesful=0;
        int records_failed=0;
        int row = 1; //Keep track of which row in Database is being processed(debugging purposes)
        boolean cont=true; //Boolean for while loop
        try {
            String url = "jdbc:sqlite:src/DataBase.sqlite";//creates database if none exists in src folder
            con = DriverManager.getConnection(url);
            System.out.println("Connection Succesful");

            /**
             * Only Create Table if First time running program
             * If Database was set up before hand
             * comment out Create_Database(con)
             */
            Create_Database(con);

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

        Scanner scanner = new Scanner(new File("src/ms3interview.csv"));
        scanner.useDelimiter("\n");//Parse into lines
        scanner.next();
        //Parse through csv
        while(cont) {
            boolean b = true;//Send to DB if condition is true
            String input = scanner.next();
            records_recieved++;
            List<String> InputArray = new LinkedList<>(Arrays.asList(input.split(",",-1)));
            int size =InputArray.size();
            /**
             * Assumption 1: All properly formated records
             * will have an image address begining with "data:image/png,";
             * consequently split will seperate the entire address into two seperate columns
             * Use Two checks to ensure record is valid: no non-empty columns and 11 columns total
             **/
            for(int i=0;i<size;i++){
                if (InputArray.get(i).isEmpty())b=false; //if any column is empty, do not send
            }
            if(size!=11)b=false; //Check size

            if (b) {
                //concatonate image address
                String temp = "\"" + InputArray.get(4) + InputArray.get(5)+"\"";
                /** Assumption 2: Column D will always contain an Image address
                 *
                 * Assumption3: Valid records will be split incorrectly at this point, must concatonate
                 * columns 4 and 5 for for a proper entry
                 */
                InputArray.remove(5);
                InputArray.set(4,temp);
                String sql = "INSERT INTO Fields(A,B,C,D,E,F,G,H,I,J) VALUES(?,?,?,?,?,?,?,?,?,?)";


                try (PreparedStatement pstmt = con.prepareStatement(sql)) {
                    pstmt.setString(1, InputArray.get(0));
                    pstmt.setString(2, InputArray.get(1));
                    pstmt.setString(3, InputArray.get(2));
                    pstmt.setString(4, InputArray.get(3));
                    pstmt.setString(5, InputArray.get(4));
                    pstmt.setString(6, InputArray.get(5));
                    pstmt.setString(7, InputArray.get(6));
                    pstmt.setString(8, InputArray.get(7));
                    pstmt.setString(9, InputArray.get(8));
                    pstmt.setString(10, InputArray.get(9));
                    pstmt.executeUpdate();
                } catch (SQLException e) {
                    System.out.println(e.getMessage());
                }
            records_succesful++; //If we have reached this point we have succesfully inserted into DB
            }
            else {
                pw.write(input+"\n");
                records_failed++;// else Records failed is incremented
            }
            row++;
        if(!scanner.hasNext()) cont = false;
        }
        scanner.close();
        pw.close();
        pw = new PrintWriter(new File("Log<"+timeStamp+">.txt"));
        System.out.println("Records Recieved: "+records_recieved + "\n" + "Records Succeded: "+ records_succesful + "\n" + "Records Failed:" + records_failed);
        pw.write("Records Recieved: "+records_recieved + "\n" + "Records Succeded: "+ records_succesful + "\n" + "Records Failed:" + records_failed);
        pw.close();
    try{
        con.close();
    }catch (Exception e){
        System.out.println(e);
    }
    }



}


