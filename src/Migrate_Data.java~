import java.io.FileNotFoundException;
import java.io.File;
import java.util.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;

public class Migrate_Data {
    public static void main(String[] args) throws FileNotFoundException {
        //Create connection to DB
        String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date());
        PrintWriter pw = new PrintWriter(new File("bad-data-<"+timeStamp+">.csv"));
        Connection con = null;
        int records_recieved=0;
        int records_succesful=0;
        int records_failed=0;
        int row = 1; //For debugging
        boolean cont=true;
        try {
            String url = "jdbc:sqlite:src/identifier.sqlite";
            con = DriverManager.getConnection(url);
            System.out.println("Connection Succesful");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

        Scanner scanner = new Scanner(new File("src/ms3interview.csv"));
        scanner.useDelimiter("\n");//Parse into lines
        scanner.next(); //Remove Header

        //Parse through csv
        while(cont) {
            //System.out.println(row);
            boolean b = true;//Send to DB condition
            String input = scanner.next();
            records_recieved++;
            List<String> InputArray = new LinkedList<String>(Arrays.asList(input.split(",",-1)));
            //System.out.println(InputArray);
            //Ensure Image Address is in one entry
            int size =InputArray.size();
            //check if any column has no entry
            for(int i=0;i<size;i++){
                if (InputArray.get(i).isEmpty())b=false;
            }
            if(InputArray.size()!=11)b=false;

            if (b) {
                //concatonate image address
                String temp = "\"" + InputArray.get(4) + InputArray.get(5)+"\"";

                InputArray.set(4,temp);
                InputArray.remove(5);
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
            records_succesful++;
            }
            else {
                pw.write(input+"\n");
                records_failed++;//Ensure records failed is incremented
            }
            row++;
        if(scanner.hasNext()==false) cont = false;
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

