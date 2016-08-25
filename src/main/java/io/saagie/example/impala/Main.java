package io.saagie.example.impala;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.logging.Logger;

public class Main {


    private static final Logger logger = Logger.getLogger("io.saagie.example.impala.Main");
    private static final String JDBC_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
    private static String connectionUrl;
    private static String user;
    private static String password;

    public static void main(String[] args) throws IOException {

        if (args.length < 3) {
            logger.severe("3 arg are required :\n\t- connectionurl ex: jdbc:hive2://impalahost:21050/ \n" +
                                  "\t-user\n" +
                                  "\t-password");
            System.err.println("3 args are required :\n\t-connectionurl  ex: jdbc:hive2://impalahost:21050/\n" +
                                       "\t-user\n" +
                                       "\t-password");
            System.exit(128);
        }
        // Get url Connection
        connectionUrl = args[0];
        // Get user
        user = args[1];
        // Get password
        password = args[2];

        // Init Connection
        Connection con = null;

        String sqlStatementDrop = "DROP TABLE IF EXISTS helloworld";
        String sqlStatementCreate = "CREATE TABLE helloworld (message String) STORED AS PARQUET";
        String sqlStatementInsert = "INSERT INTO helloworld VALUES (\"helloworld\")";
        String sqlStatementSelect = "SELECT * from helloworld";
        String sqlStatementInvalidate = "INVALIDATE METADATA";
        try {
            // Set JDBC Impala Driver
            Class.forName(JDBC_DRIVER_NAME);
            // Connect to Impala
            con = DriverManager.getConnection(connectionUrl,user,password);
            // Init Statement
            Statement stmt = con.createStatement();
            // Invalidate metadata to update changes
            stmt.execute(sqlStatementInvalidate);
            // Execute DROP TABLE Query
            stmt.execute(sqlStatementDrop);
            logger.info("Drop Impala table with security : OK");
            // Execute CREATE Query
            stmt.execute(sqlStatementCreate);
            logger.info("Create Impala table with security : OK");
            // Execute INSERT Query
            stmt.execute(sqlStatementInsert);
            logger.info("Insert into Impala table with security : OK");
            // Execute SELECT Query
            ResultSet rs = stmt.executeQuery(sqlStatementSelect);
            while(rs.next()) {
                logger.info(rs.getString(1));
            }
            logger.info("Select from Impala table with security : OK");
            // Invalidate metadata to update changes
            stmt.execute(sqlStatementInvalidate);

        } catch (Exception e) {
            logger.severe(e.getMessage());
        } finally {
            try {
                con.close();
            } catch (Exception e) {
                // swallow
            }
        }



    }
}
