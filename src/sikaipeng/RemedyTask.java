package sikaipeng;

import java.sql.*;
import java.util.Date;
/**
 * Created by sikpeng on 3/16/2016.
 */
public class RemedyTask {

    private static Connection connRemedyDB;
    private static Connection connBMSCIDB;
    private static PreparedStatement sql_statement_insert_data = null;
    private static Statement sql_statement_get_remedy_request = null;
    private static Statement sql_statement_clear = null;
    private static String jdbc_clear_sql;


    public void executeRemedyTask(){
        System.out.println("Remedy task starts running at "+ new Date());

        try {
            Class.forName ("oracle.jdbc.OracleDriver");
            connRemedyDB = DriverManager.getConnection("jdbc:oracle:thin:@//173.37.226.167:1541/ITSTPRD_RO.cisco.com","R8BRO", "r61QOvqBoMh");
            connBMSCIDB = DriverManager.getConnection("jdbc:oracle:thin:@rtp-dbpl-cibld.cisco.com:1521:BMSCIDB1","ecloud", "ecloud");

            System.out.println("Remedy database connection is opened.");
            System.out.println("BMSCI database connection is opened.");

            sql_statement_get_remedy_request = connRemedyDB.createStatement();


            String jdbc_insert_BMSCI_sql = "INSERT INTO HOST_REMEDY"
                    + "(ASSOCIATIONS_ID, SUBMITTER, SUBMIT_DATE, SCHEDULED_START_DATE, SCHEDULED_END_DATE, STATUS, REQUEST_ID02, REQUEST_DESCRIPTION01, ASSOCIATION_TYPE01, REQUEST_TYPE01, CIS_CHANGEALIAS, CIS_ENVIRONMENT, CIS_MEMS_CI_TYPE, LASTUPDATED ) VALUES "
                    + "(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            sql_statement_insert_data = connBMSCIDB.prepareStatement(jdbc_insert_BMSCI_sql);


            String getRequestSQL = "select AR.ASSOCIATIONS_ID, AR.SUBMITTER, ARSYSTEM.EPOCH_TO_PST_CONV_DATE(AR.SUBMIT_DATE) as SUBMIT_DATE,  ARSYSTEM.EPOCH_TO_PST_CONV_DATE(INF.SCHEDULED_START_DATE) AS SCHEDULED_START_DATE, " +
                    "ARSYSTEM.EPOCH_TO_PST_CONV_DATE(INF.SCHEDULED_END_DATE) AS SCHEDULED_END_DATE, E1.ENUM_VALUE as STATUS, AR.REQUEST_ID02, AR.REQUEST_DESCRIPTION01, " +
                    "E2.ENUM_VALUE as ASSOCIATION_TYPE01, E3.ENUM_VALUE as REQUEST_TYPE01, AR.CIS_CHANGEALIAS, AR.CIS_ENVIRONMENT, AR.CIS_MEMS_CI_TYPE " +
                    "from ARSYSTEM.V_CHG_ASSOCIATIONS AR, ARSYSTEM.V_CHG_INFRASTRUCTURE_CHANGE INF, ARSYSTEM.RO_FIELD_ENUM_VALUES E1, ARSYSTEM.RO_FIELD_ENUM_VALUES E2, ARSYSTEM.RO_FIELD_ENUM_VALUES E3 " +
                    "where (AR.REQUEST_DESCRIPTION01 like '%apl-bld%' " +
                    "or AR.REQUEST_DESCRIPTION01 like '%apl-art%' " +
                    "or AR.REQUEST_DESCRIPTION01 like '%apl-sqs%' " +
                    "or AR.REQUEST_DESCRIPTION01 like '%apl-stats%') and AR.CIS_MEMS_CI_TYPE = 'IT Host Name' " +
                    "and ( E1.VIEW_NAME = 'V_CHG_ASSOCIATIONS' and E1.FIELD_NAME ='STATUS' and AR.STATUS = E1.ENUMID ) " +
                    "and ( E2.VIEW_NAME = 'V_CHG_ASSOCIATIONS' and E2.FIELD_NAME = 'ASSOCIATION_TYPE01' and AR.ASSOCIATION_TYPE01 = E2.ENUMID ) " +
                    "and ( E3.VIEW_NAME = 'V_CHG_ASSOCIATIONS' and E3.FIELD_NAME = 'REQUEST_TYPE01' and AR.REQUEST_TYPE01 = E3.ENUMID ) " +
                    "and ( AR.REQUEST_ID02 = INF.INFRASTRUCTURE_CHANGE_ID ) ";
            StringBuffer output = new StringBuffer();

            jdbc_clear_sql = "TRUNCATE TABLE HOST_REMEDY";
            sql_statement_clear = connBMSCIDB.createStatement();
            ResultSet resultSet = sql_statement_get_remedy_request.executeQuery(getRequestSQL);
            sql_statement_clear.executeQuery(jdbc_clear_sql);
            System.out.println("Old HOST_REMEDY table is cleared.");
            int numRowsUpdated = 0;
            Date date = new Date();
            long timeStamp = date.getTime();
            Timestamp sqlTimestamp = new Timestamp(timeStamp);

            while( resultSet.next() ){

                sql_statement_insert_data.setString(1, resultSet.getString("ASSOCIATIONS_ID"));
                sql_statement_insert_data.setString(2, resultSet.getString("SUBMITTER"));
                sql_statement_insert_data.setString(3, resultSet.getString("SUBMIT_DATE"));
                sql_statement_insert_data.setString(4, resultSet.getString("SCHEDULED_START_DATE"));
                sql_statement_insert_data.setString(5, resultSet.getString("SCHEDULED_END_DATE"));
                sql_statement_insert_data.setString(6, resultSet.getString("STATUS"));
                sql_statement_insert_data.setString(7, resultSet.getString("REQUEST_ID02"));
                sql_statement_insert_data.setString(8, resultSet.getString("REQUEST_DESCRIPTION01"));
                sql_statement_insert_data.setString(9, resultSet.getString("ASSOCIATION_TYPE01"));
                sql_statement_insert_data.setString(10, resultSet.getString("REQUEST_TYPE01"));
                sql_statement_insert_data.setString(11, resultSet.getString("CIS_CHANGEALIAS"));
                sql_statement_insert_data.setString(12, resultSet.getString("CIS_ENVIRONMENT"));
                sql_statement_insert_data.setString(13, resultSet.getString("CIS_MEMS_CI_TYPE"));
                sql_statement_insert_data.setTimestamp(14,sqlTimestamp);


                sql_statement_insert_data.executeUpdate();
                numRowsUpdated ++;
            }


            System.out.println(numRowsUpdated + " rows of records are updated.");

            //Close prepared statement
            sql_statement_get_remedy_request.close();
            //COMMIT transaction
            connRemedyDB.commit();
            //Close connection
            connRemedyDB.close();
            System.out.println("Remedy database connection is closed.");
            connBMSCIDB.close();
            System.out.println("BMSCI database connection is closed.");
            System.out.println("Remedy task is completed at " + new Date());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
