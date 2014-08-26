package it.arg.seven;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import oracle.jdbc.OracleTypes;


public class OraConnection {

	private static Connection conn;
	private static CallableStatement config_stmt;
	
	private static CallableStatement procLogAppException;
	private static CallableStatement procLogInfo;
	
	private static boolean _fromIntern = true;
	private static boolean _infolog = false;
	
	/**
	 * Restituisce una connessione al db oracle corrente
	 * @return
	 * @throws SQLException
	 */
	public static Connection getConnection() throws SQLException {
		
		if (conn==null) {
			conn = DriverManager.getConnection("jdbc:default:connection:");
			intiWtlLogger();
		}
		
		return conn;
		
	}
	
	public static Connection getConnection(String cs, String userName, String password, boolean infolog) throws SQLException {
		
		if (conn==null) {
			DriverManager.registerDriver (new oracle.jdbc.driver.OracleDriver());
			conn = DriverManager.getConnection("jdbc:oracle:thin:@"+cs, userName, password);
			_fromIntern = false;
			_infolog = infolog;
		}
		return conn;
		
	}
	
	/**
	 * Inizializzazione delle procedure di logging
	 */
	private static void intiWtlLogger() throws SQLException {
		
		procLogAppException = conn.prepareCall("{ call wtl_logger.logappexception(?, ?, ?) }");
		
		procLogInfo = conn.prepareCall("{ call wtl_logger.infolog(?, ?) }");
		
	}
	
	/**
	 * Recupero di un parametro dalla tabella tb_config nello schema suetl01
	 * @param param
	 * @return
	 * @throws SQLException
	 */
	public static String getConfig(String param) throws SQLException {
		
		// Forzatura della connessine al db oracle
		getConnection();
		
		// Verifica l'esistenza dello statement
		if (config_stmt == null) {
			config_stmt = conn.prepareCall("{ ? = call suetl01.wtlpk_util.getparm(?)}");
			config_stmt.registerOutParameter(1, OracleTypes.VARCHAR);
		}
		
		// Call dello statement
		config_stmt.setString(2, param);
		config_stmt.execute();
		
		return config_stmt.getString(1);
		
		
	}
	
	/**
	 * Log dell'exception nel motore etl
	 * @param message		Messaggio dell'eccezzione
	 * @throws Exception
	 */
	public static void LogAppException(String contesto, String message) {
		LogAppException(-20009, contesto, message);
	}
	
	/**
	 * Log dell'exception nel motore etl
	 * @param code			Codice dell'eccezzione
	 * @param message		Messaggio dell'eccezzione
	 * @throws Exception
	 */
	public static void LogAppException(int code, String contesto, String message) {
		
		if (_fromIntern) {
			try {
				procLogAppException.setInt(1, code);
				procLogAppException.setString(2, "DB2Extractor."+contesto);
				procLogAppException.setString(3, message);
				procLogAppException.execute();
			}
			catch (SQLException e) {

			}
		}
		else {
			System.out.println(contesto + " - " + message);
		}
	}
	
	/**
	 * Log di un informazione nel motore etl
	 * @param message		Messaggio dell'eccezzione
	 * @throws Exception
	 */
	public static void InfoLog(String contesto, String message) {
		
		if (_fromIntern) {
			try {
				procLogInfo.setString(1, "DB2Extractor."+contesto);
				procLogInfo.setString(2, message);
				procLogInfo.execute();
			}
			catch (SQLException e) {
				
			}
		}
		else {
			if (_infolog)
				System.out.println(contesto + " - " + message);
		}
	}
	
}