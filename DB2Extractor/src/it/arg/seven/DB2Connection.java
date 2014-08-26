package it.arg.seven;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
 

public class DB2Connection {

	private static Connection conn;
	
	/**
	 * Restituisce la connessione al DB2 su as400
	 * @return
	 * @throws SQLException
	 */
	public static Connection getConnection() throws SQLException {
		
		if (conn==null) {
			DriverManager.registerDriver (new com.ibm.as400.access.AS400JDBCDriver());
			conn = DriverManager.getConnection(OraConnection.getConfig("CONNECTION_STRING_DB2"), OraConnection.getConfig("USERNAME_DB2"), OraConnection.getConfig("PASSWORD_DB2"));
		}
		return conn;
		
	}
	
	public static Connection getConnection(String cs, String username, String password) throws SQLException {
		
		if (conn==null) {
			DriverManager.registerDriver (new com.ibm.as400.access.AS400JDBCDriver());
			conn = DriverManager.getConnection(cs, username, password);
		}
		return conn;
		
	}
	
}
