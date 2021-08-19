package com.gaig.utility;

import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Properties;

public class SyncCubeToAcctSrvc {

    static String CUBE_SQL = "SELECT s.SUBMISSION_ID, bd.BUS_DIVISION_CD "
            + "FROM RAMT.SUBMISSION s, RAMT.BUS_DIVISION bd "
            + "WHERE s.BUS_DIVISION_ID = bd.BUS_DIVISION_ID "
            + "AND s.POL_TERM_EFF_DT >= '01 MAR 2019' "
            + "AND FIND_UD_REF_VALUE(s.TRANS_TYPE_REF_ID, 'ITEM_CD') IN ('NBS', 'NBP', 'REN')"
            + "ORDER BY s.SUBMISSION_ID";
    static String AS_SQL   = "SELECT s.* FROM COREUWACCTSVC.SUBMISSION s "
            + "WHERE s.TRANSACTION_TYPE IN  ('NBS', 'NBP', 'REN') "
            + "AND s.effective_date  >= '01 Mar 2019' "
            + "ORDER BY SOURCE_SUBMISSION_ID";


    public static void main(String[] args) {
        try {
            Properties properties = new Properties();
            FileInputStream in = new FileInputStream("application-local.properties");
            properties.load(in);
            in.close();
            String CUBE_HOST = (String) properties.get("cube.host");
            String CUBE_PORT = (String) properties.get("cube.port");
            String CUBE_SERVICE = (String) properties.get("cube.service");
            String CUBE_USER = (String) properties.get("cube.user");
            String CUBE_PASS = (String) properties.get("cube.pass");
            String AS_HOST = (String) properties.get("as.host");
            String AS_PORT = (String) properties.get("as.port");
            String AS_SERVICE = (String) properties.get("as.service");
            String AS_USER = (String) properties.get("as.user");
            String AS_PASS = (String) properties.get("as.pass");
            String CUBE_URL = "jdbc:oracle:thin:@//" + CUBE_HOST + ":" + CUBE_PORT + "/" + CUBE_SERVICE;
            String AS_URL   = "jdbc:oracle:thin:@//" + AS_HOST + ":" + AS_PORT + "/" + AS_SERVICE;
            //ArrayList <String> acctSvcSourceSubIds = new ArrayList<String>(Arrays.asList("10704941","10813976","11161118","11182439"));   //this is for testing to make sure match/merge logic below is working.
            ArrayList <String> acctSvcSourceSubIds =  accountServiceSubIds(AS_USER, AS_PASS, AS_URL, AS_SQL);
            ArrayList<LinkedHashMap<String, String>> cubeSubs = cubeSubmissionIds(CUBE_USER, CUBE_PASS, CUBE_URL, CUBE_SQL);
            int noMatchCount = matchCubeToAcctService(acctSvcSourceSubIds, cubeSubs);
            System.out.println("total subids in cube with no accound service match = " + noMatchCount);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private static ArrayList<String> accountServiceSubIds(String AS_USER, String AS_PASS, String AS_URL, String AS_SQL) {
        ArrayList <String> acctSvcSourceSubIds = new ArrayList<String>();

        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            Connection con= DriverManager.getConnection(AS_URL, AS_USER, AS_PASS);
            Statement stmt=con.createStatement();
            ResultSet rs=stmt.executeQuery(AS_SQL);
            int countAs = 0;
            while(rs.next()) {
                acctSvcSourceSubIds.add(rs.getString(8) );
                if (countAs % 10000 == 0) {
                    System.out.println("countAs->" + countAs);
                }
                //if (countAs > 20) break;     //comment this out in a real run
                countAs++;
            }
            System.out.println("size of account service submission list=>" + acctSvcSourceSubIds.size());
            con.close();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return acctSvcSourceSubIds;
    }



    private static ArrayList<LinkedHashMap<String, String>> cubeSubmissionIds(String CUBE_USER, String CUBE_PASS, String CUBE_URL,String CUBE_SQL) {
        ArrayList<LinkedHashMap<String, String>> cubeSubIds = new ArrayList <LinkedHashMap<String, String>>();
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            Connection con= DriverManager.getConnection(CUBE_URL, CUBE_USER, CUBE_PASS);
            Statement stmt=con.createStatement();
            ResultSet rs=stmt.executeQuery(CUBE_SQL);
            int countCube = 0;
            while(rs.next()) {
                LinkedHashMap <String, String> cubeSub = new LinkedHashMap <String, String> ();
                cubeSub.put("submissionId", rs.getString(1) );
                cubeSub.put("businessDivisionCode", rs.getString(2) );
                cubeSubIds.add(cubeSub);
                if (countCube % 10000 == 0) {
                    System.out.println("countCube->" + countCube);
                }
                //if (countCube > 20) break;   //comment this out in a real run
                countCube++;
            }
            System.out.println("size of cube submission list=>" + cubeSubIds.size());
            con.close();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return cubeSubIds;
    }


    private static int matchCubeToAcctService(ArrayList<String> acctSvcSourceSubIds, ArrayList<LinkedHashMap<String, String>> cubeSubs) {
        int noMatchCount = 0;
        try {

            FileWriter myWriter = new FileWriter("CubeSubsNotInAcctSrvces.csv");
            myWriter.write("'submissionId','businessDivisionCode'"+ "\n");
            for (LinkedHashMap<String, String> cubeSub : cubeSubs) {
                String cubeSubId = cubeSub.get("submissionId");
                String cubeBusDivCd = cubeSub.get("businessDivisionCode");
                if (!acctSvcSourceSubIds.contains(cubeSubId)){
                    //System.out.println("BU=>" + cubeBusDivCd + "sub = > " + cubeSubId );
                    myWriter.write("'" + cubeSubId  + "','" + cubeBusDivCd + "'" + "\n");
                    noMatchCount++;
                }
            }
            myWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return noMatchCount;
    }



}
