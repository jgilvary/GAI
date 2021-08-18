package com.gaig.utility;
import java.sql.*;
import java.util.ArrayList;

public class Syncer {

    public static void main(String[] args) {

        String HOST = "websrvcd-dev.td.afg";
        String PORT ="1521";
        String SERVICE ="app_websrvcd.dev.gai.com";
        //String URL = "jdbc:oracle:thin:@" + HOST + ":" + PORT + ":" + SERVICE;
        String URL = "jdbc:oracle:thin:@//" + HOST + ":" + PORT + "/" + SERVICE;
        String USER = "coreuwacctsvc";
        String PASS = "zoP8lzeh0xo3pUro";
        //String SQL = "select SOURCE_SUBMISSION_ID from SUBMISSION where TRANSACTION_TYPE in  ('NBS', 'NBP', 'REN') AND effective_date  >= '01 Mar 2019' AND effective_date  < '03 Mar 2019' order by SOURCE_SUBMISSION_ID; ";
        //String SQL = "SELECT s.* FROM COREUWACCTSVC.SUBMISSION s where s.TRANSACTION_TYPE in  ('NBS', 'NBP', 'REN') AND s.effective_date  >= '01 Mar 2019' AND s.effective_date  < '03 Mar 2019' order by SOURCE_SUBMISSION_ID";
        String SQL = "SELECT s.* FROM COREUWACCTSVC.SUBMISSION s where s.TRANSACTION_TYPE in  ('NBS', 'NBP', 'REN') AND s.effective_date  >= '01 Mar 2019' order by SOURCE_SUBMISSION_ID";

        ArrayList <String> acctSvcSourceSubIds = new ArrayList<String> ();


        try {

            //acct srvce
            Class.forName("oracle.jdbc.driver.OracleDriver");
            Connection con=DriverManager.getConnection(URL, USER, PASS);
            Statement stmt=con.createStatement();
            //ResultSet rs=stmt.executeQuery("select SOURCE_SUBMISSION_ID from SUBMISSION where TRANSACTION_TYPE in  ('NBS', 'NBP', 'REN') AND effective_date  >= '01 Mar 2019' AND effective_date  < '03 Mar 2019' order by SOURCE_SUBMISSION_ID; ");
            ResultSet rs=stmt.executeQuery(SQL);
            while(rs.next()) {
                acctSvcSourceSubIds.add(rs.getString(8) );
//                System.out.println(rs.getInt(1) + "  "
//                        + rs.getString(4) + "  "
//                        + rs.getString(8) + "  "
//                        + rs.getString(12));

            }
            con.close();

            //cube

            int i = 1;

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

    }

}
