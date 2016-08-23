package io.saagie.example.impala;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;

public class Main {


	private static final Logger logger = Logger.getLogger("io.saagie.example.impala.ReadWrite");
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

		//==== Select data from Impala Table
		String sqlStatement = "SELECT * FROM helloworld";
		try {
			// Set JDBC Driver
			Class.forName(JDBC_DRIVER_NAME);
			// Connect to Hive/Impala with security
			con = DriverManager.getConnection(connectionUrl,user,password);
			// Init Statement
			Statement stmt = con.createStatement();
			// Execute Query
			ResultSet rs = stmt.executeQuery(sqlStatement);

		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				con.close();
			} catch (Exception e) {
				// swallow
			}
		}

		//==== Insert data into Impala Table
		sqlStatement = "INSERT INTO helloworld2 SELECT * FROM helloworld";
		try {
			// Set JDBC Driver
			Class.forName(JDBC_DRIVER_NAME);
			// Connect to Hive/Impala with security
			con = DriverManager.getConnection(connectionUrl,user,password);
			// Init Statement
			Statement stmt = con.createStatement();
			// Execute Query
			stmt.execute(sqlStatement);
			// Invalidate metadata to update changes
			stmt.execute("INVALIDATE METADATA");

		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				con.close();
			} catch (Exception e) {
				// swallow
			}
		}


	}
}
