package sikaipeng;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.*;
import java.util.Date;

/**
 * Created by sikpeng on 2/24/2016.
 */
public class EmanTask extends TimerTask {

    private static Connection conn;
    private static PreparedStatement sql_statement = null;
    private static Statement sql_statement_clear = null;
    private static String jdbc_clear_sql;
    private int numRowsUpdated;

    public static void main(String[] args) {
        System.out.println("Application boots up at "+ new Date()+" , wait a few minutes for task to run.");
        Timer timer = new Timer();
        long firstDelay = 1*60*1000; //first daley 1 min after the application boot
        long period = 5*60*1000; //24*60*60*1000 add 24 hours delay between job executions.
        EmanTask emanTask = new EmanTask();
        timer.schedule(emanTask, firstDelay, period);
    }

    //add scheduled task here
    @Override
    public void run() {
        executeEmanTask();
        RemedyTask remedyTask = new RemedyTask();
        remedyTask.executeRemedyTask();

    }

    private void executeEmanTask() {

        System.out.println("Eman task starts running at "+ new Date());
        try {
            Class.forName ("oracle.jdbc.OracleDriver");
            conn = DriverManager.getConnection("jdbc:oracle:thin:@rtp-dbpl-cibld.cisco.com:1521:BMSCIDB1","ecloud", "ecloud");
            System.out.println("Database connection is opened.");

            jdbc_clear_sql = "TRUNCATE TABLE HOST_EMAN";
            sql_statement_clear = conn.createStatement();

            String jdbc_insert_sql = "INSERT INTO HOST_EMAN"
                    + "(HOSTNAME, APPLICATION, BUILDING, COMMENTS, CONTACT, LASTUPDATED) VALUES"
                    + "(?,?,?,?,?,?)";
            sql_statement = conn.prepareStatement(jdbc_insert_sql);

            EmanTask obj = new EmanTask();
            String command = "/ecs/bin/eman-cli host show --hostname=%apl-bld%,%apl-art%,%apl-sqs%,%apl-stats% --display=hostname,application,building_id,comments,contact --csv";

            StringBuffer output = new StringBuffer();
            Process p;
            p = Runtime.getRuntime().exec(command);
            //p.waitFor();
            BufferedReader reader =
                    new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line = "";


            sql_statement_clear.executeQuery(jdbc_clear_sql);
            System.out.println("Old HOST_EMAN table is cleared.");
            numRowsUpdated = 0;
            Date date = new Date();
            long timeStamp = date.getTime();
            Timestamp sqlTimestamp = new Timestamp(timeStamp);

            while ((line = reader.readLine())!= null) {
                filterAndInsertHost(line, sqlTimestamp);
            }

            System.out.println(numRowsUpdated + " rows of records are updated.");


            //Close prepared statement
            sql_statement.close();
            //COMMIT transaction
            conn.commit();
            //Close connection
            conn.close();
            System.out.println("Database connection is closed.");
            System.out.println("Eman task is completed at " + new Date());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void filterAndInsertHost(String line, Timestamp time){

        //split by comma, except those commas in double quotes
        String[] fields =  line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);
        String hostName = fields[0];
        String application = fields[1];
        String building = fields[2];
        String comment = fields[3];
        String contacts = fields[4];

        String shortContact = getMatchedShortContact(contacts);

        if(shortContact != null){

            try {
                sql_statement.setString(1,hostName);
                sql_statement.setString(2,application);
                sql_statement.setString(3,building);
                sql_statement.setString(4,comment);
                sql_statement.setString(5,shortContact);
                sql_statement.setTimestamp(6,time);

                // execute the insert statement
                sql_statement.executeUpdate();
                numRowsUpdated ++;

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }


    /* this method will check whether a host is our host, based on contacts information,

    A sample contact info looks like this

         Hostname : ntn-wapl-sqs1.cisco.com
         Contact : [7] cchurchi [EM] Accountable Director
         Contact : [9] EHS-Tier2 [OT] Alliance Assignment Group
         Contact : [3] ecs-ea [MA] Change Alias
         Contact : [4] build-mgmt-dev [MA] Change Alias
         Contact : [5] bcourlis [EM] Client POC
         Contact : [8] avenkita [EM] Responsible Manager
         Contact : [1] ecs-ea [MA] Root Mail
         Contact : [2] ehs-tier2 [MA] Root Mail
         Contact : [10] ecs-smsd [MA] SFO Approver

        exame each contactItem for the following conditions

        '[EM] Client POC' in (matholt, bcourlis, recroft, mtoothak, tayjones, prayyapp, chjagann)
            OR
        '[MA] Change Alias' in (bmsadmin, build-mgmt-dev)
            OR
        '[MA] Client POC' in (bmsadmin, build-mgmt-dev)

     if yes, return the matching contact, if not, return null.
    */
    private String getMatchedShortContact(String contacts){
        //first split contacts into each contact
        String[] contactItem = contacts.split(",");
        List<String> clientPOCList = new ArrayList<String>(Arrays.asList("matholt", "bcourlis", "recroft", "mtoothak", "tayjones", "prayyapp","chjagann"));
        List<String> mailerList = new ArrayList<String>(Arrays.asList("bmsadmin","build-mgmt-dev"));
        String shortContact = null;
        String role_EM_Client_POC = "[EM] Client POC";
        String role_MA_Change_Alias = "[MA] Change Alias";
        String role_MA_Client_POC = "[MA] Client POC";

        for(int i = 0; i < contactItem.length; i++) {
            //check [EM] Client POC
            if(contactItem[i].endsWith(role_EM_Client_POC)){
               shortContact = findMatch(contactItem[i], role_EM_Client_POC, clientPOCList);
                if(shortContact != null){
                    return shortContact;
                }
            }
            //check [MA] Change Alias
            if(contactItem[i].endsWith(role_MA_Change_Alias)){
                shortContact =  findMatch(contactItem[i],role_MA_Change_Alias, mailerList);
                if(shortContact != null){
                    return shortContact;
                }
            }
            //check [MA] Client POC
            if(contactItem[i].endsWith(role_MA_Client_POC)){
                shortContact =  findMatch(contactItem[i],role_MA_Client_POC, mailerList);
                if(shortContact != null){
                    return shortContact;
                }
            }
        }
        //after the loop, if still cannot meet conditions, return null;
        return null;
    }


    private String findMatch(String contactItem, String contactRole, List<String> matchingList){
        int beginIndex = contactItem.indexOf("]") + 1;
        int endIndex = contactItem.indexOf(contactRole);
        String contactName = contactItem.substring(beginIndex,endIndex).trim();
        if(matchingList.contains(contactName)) {
            return contactName + " " + contactRole;
        } else {
            return null;
        }
    }




}
