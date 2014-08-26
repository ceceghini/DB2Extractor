package it.arg.seven;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;


public class DB2Cursor {

	private String _sql;
	private String _callp;
	
	private Connection _orac;
	private Connection _db2c;
	private Statement _stmt;
	private CallableStatement _cStmt;
	
	public DB2Cursor(String sql, String callp) throws Exception {
		
//			OraConnection.InfoLog(this.getClass().toString(), "Inizializzazione DB2Cursor");
		
		this._sql = sql;
		this._callp = callp;
		
		// Recupero le connessioni al db oracle e al db db2
		_orac = OraConnection.getConnection();
		
			OraConnection.InfoLog(this.getClass().toString(), "Connessione al db oracle effettutata.");
		
		_db2c = DB2Connection.getConnection();
		
			OraConnection.InfoLog(this.getClass().toString(), "Connessione al db DB2 SQL Server effettutata.");
		
		// Statement
		_stmt = _db2c.createStatement();	// Statement di recupero dei dati
		_cStmt = _orac.prepareCall(_callp);	// Statement di inserimento dei datis
		
			OraConnection.InfoLog(this.getClass().toString(), "Preparazione statement sql.");
		
	}
	
	/**
	 * Esecuzione del cursore su db2 ed esecuzione della proc di inserimento in oracle
	 */
	/**
	 * 
	 */
	/**
	 * 
	 */
	public void Execute() {
		
		int j = 0;
		int iCol = 0;
		
		try {
			
				OraConnection.InfoLog(this.getClass().toString()+".Execute", "Esecuzione sql su db2.");
			
			// Esecuzione dello statement db2 e loop fra il risultato
			ResultSet rs = _stmt.executeQuery(_sql);
			ResultSetMetaData rsm = rs.getMetaData();
			
				OraConnection.InfoLog(this.getClass().toString()+".Execute", "Sql eseguito correttamente.");
		
			int numColumns = rsm.getColumnCount();
			Map<Integer,String> map = new HashMap<Integer,String> ();
	
			// Memorizzazione in una hashmap delle tipologia di colonna
			for (int i=1;i<=numColumns;i++) {
				map.put(i, rsm.getColumnTypeName(i));
			}
				
				OraConnection.InfoLog(this.getClass().toString()+".Execute", "Lunghezza colonne recuperata. NUM_COLS: [" + numColumns + "]");
		
			while (rs.next()) {
				
				j ++;
				
				OraConnection.InfoLog(this.getClass().toString()+".Execute", "Elaborazione record n° " + j);
								
				// Loop fra tutte le colonne del cursore
				for (iCol=1;iCol<=numColumns;iCol++) {
								
					if (map.get(iCol).compareTo("CHAR")==0)
						_cStmt.setString(iCol, rs.getString(iCol));
					else if (map.get(iCol).compareTo("NUMERIC")==0 | map.get(iCol).compareTo("DECIMAL")==0)
						_cStmt.setBigDecimal(iCol, rs.getBigDecimal(iCol));
					else if (map.get(iCol).compareTo("DATE")==0)
						_cStmt.setDate(iCol, rs.getDate(iCol));
					else
						OraConnection.LogAppException(this.getClass().toString()+".Execute", "Mapping della tipologia di dato della colonna non riuscito. COLUMN: ["+ rsm.getColumnName(iCol) +"] - TYPE: ["+ rsm.getColumnTypeName(iCol) +"]");
					//else if (i==Types.DATE)
					//	cStmt.setDate(i, rs.getDate(i));
					
				}
				
				_cStmt.execute();
				
				OraConnection.InfoLog(this.getClass().toString()+".Execute", "Esecuzione procedura di caricamentoe eseguita.");
				
			}
			
			/* 19.01.2010 - cesare
			 * Non effettuando il close del recordset in caso di elaborazioni più lunghe veniva generato un errore di max_open_cursor superato
			 **/
			rs.close();
			
			
		}
		catch (SQLException e) {
			OraConnection.LogAppException(this.getClass().toString()+".Execute", getStackTrace(e) + '\n' + "RIGA: ["+j+"] - COLONNA: ["+iCol+"]");
		}
		
	}
	
	/*public static void run(String sql, String callp) throws Exception {
		
		DB2Cursor c= new DB2Cursor(sql, callp);
		
		c.Execute();
		
	}*/
	
	private static String getStackTrace(Throwable throwable) {
	    Writer writer = new StringWriter();
	    PrintWriter printWriter = new PrintWriter(writer);
	    throwable.printStackTrace(printWriter);
	    return writer.toString();
	}
	
}
