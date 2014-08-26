package it.arg.test;

import it.arg.seven.DB2Connection;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class DebugSql {

	private static String pad(String str, int size, char padChar)
	{
		
	  if (str==null)
		  str = " ";
		
	  StringBuffer padded = new StringBuffer(str);
	  while (padded.length() < size)
	  {
	    padded.append(padChar);
	  }
	  return padded.toString();
	}

	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		
		// TODO Auto-generated method stub

		//System.out.println("Argomenti: " + args.length);
		//System.out.println("");
		
		//System.out.println("CS: "+args[0]);
		//System.out.println("Utente: "+args[1]);
		//System.out.println("Password: "+args[2]);
		//System.out.println("SQL: "+args[3]);
		
		boolean lite = false;
		boolean result = false;
		boolean ddl = false;
		
		if (args.length > 4) {
			if (args[4].compareTo("small")==0)
				lite = true;
			if (args[4].compareTo("result")==0)
				result = true;
			if (args[4].compareTo("ddl")==0)
				ddl = true;
		}
			
		//System.out.println("Modalita lite: " + lite);
		
		//System.out.println("");
		
		// Connessione al db
		Connection c = DB2Connection.getConnection(args[0], args[1], args[2]);
	
		// Creazione dello statement ed esecuzione
		Statement s = c.createStatement();
		
		String sql = "";
		
		if (args[3].substring(args[3].length()-4, args[3].length()).compareTo(".sql")==0) {
			
			File f = new File(args[3]);
			StringBuilder contents = new StringBuilder();
			try {
				//use buffering, reading one line at a time
			    //FileReader always assumes default encoding is OK!
			    BufferedReader input =  new BufferedReader(new FileReader(f));
			    try {
			    	String line = null; //not declared within while loop
			        /*
			        * readLine is a bit quirky :
			        * it returns the content of a line MINUS the newline.
			        * it returns null only for the END of the stream.
			        * it returns an empty String if two newlines appear in a row.
			        */
			        while (( line = input.readLine()) != null){
			          contents.append(line);
			          contents.append(System.getProperty("line.separator"));
			        }
			    }
			    finally {
			    	input.close();
			    }
			}
			catch (IOException ex){
				ex.printStackTrace();
			}
			
			sql = contents.toString();
			
		}
		else {
			sql = args[3];
		}
		
		ResultSet rs = s.executeQuery(sql);
		
		// Recupero dei metadata
		ResultSetMetaData rsm = rs.getMetaData();
		
		String line="";
		
		// Stampa il risultato dell'sql a video
		if (result) {
			
			int l;
			
			for(int i=1;i<=rsm.getColumnCount();i++) {
				
				l = rsm.getColumnDisplaySize(i);
				
				if (rsm.getColumnName(i).length()>l)
					line += rsm.getColumnName(i).substring(1, l) + " | ";
				else				
					line += pad(rsm.getColumnName(i), l, ' ') + " | ";
			}
			
			System.out.println(line);
			
			int numr = 0;
			
			while(rs.next()) {
			
				line = "";
				numr ++;
				
				//if (rsm.getColumnName(i) > rsm.getcol)
				
				for(int i=1;i<=rsm.getColumnCount();i++) {
					
					l = rsm.getColumnDisplaySize(i);
					
					line += pad(rs.getString(i), l, ' ') + " | ";
				}
				
				System.out.println(line);
				
			}
			
			System.out.println("----------------------------------------------");
			System.out.println("Numero record: " + numr);
			
			return;
		}
		
		// DDL Dell'oggetto
		if (ddl) {
			String ddlSql = "create table " + rsm.getTableName(1) + System.getProperty("line.separator");
			ddlSql += "("+System.getProperty("line.separator");
			
			for (int i=1;i<=rsm.getColumnCount();i++) {
				
				ddlSql += rsm.getColumnName(i) + " ";
				
				if (rsm.getColumnTypeName(i).compareTo("CHAR")==0) {
					if (rsm.getColumnDisplaySize(i)==1)
						ddlSql += "CHAR(1), ";
					else
						ddlSql += "VARCHAR2(" + rsm.getColumnDisplaySize(i) + "), ";
				}
				
				if (rsm.getColumnTypeName(i).compareTo("NUMERIC")==0)
					ddlSql += "NUMBER, ";
				
				if (rsm.getColumnTypeName(i).compareTo("DECIMAL")==0)
					ddlSql += "NUMBER, ";
				
				if (rsm.getColumnTypeName(i).compareTo("DATE")==0)
					ddlSql += "DATE, ";
				
				ddlSql += System.getProperty("line.separator");
				
			}
			
			ddlSql += ");"+System.getProperty("line.separator");
			ddlSql += System.getProperty("line.separator");
			
			System.out.println(ddlSql);
			return;
		}
		
		
		// Visualizzazine del tipo 
		for (int i=1;i<=rsm.getColumnCount();i++) {
			
			if (lite) {
				
				String type="";
				
				if (rsm.getColumnTypeName(i).compareTo("CHAR")==0)
					type = "VARCHAR2";
				
				if (rsm.getColumnTypeName(i).compareTo("NUMERIC")==0)
					type = "NUMBER";
				
				if (rsm.getColumnTypeName(i).compareTo("DECIMAL")==0)
					type = "NUMBER";
				
				if (rsm.getColumnTypeName(i).compareTo("DATE")==0)
					type = "DATE";
				
				System.out.println(rsm.getColumnName(i) + " " + type);
			}
			else {
				System.out.println("Colonna: [" + i + "] " + rsm.getColumnName(i));
				System.out.println("   getColumnType: " + rsm.getColumnType(i));
				System.out.println("   getColumnTypeName: " + rsm.getColumnTypeName(i));
				System.out.println("   getColumnClassName: " + rsm.getColumnClassName(i));
			}
		}
		
	}

}
