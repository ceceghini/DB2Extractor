package it.arg.seven;


public class DB2 {
	
public static void run(String sql, String callp) throws Exception {
		
		DB2Cursor c= new DB2Cursor(sql, callp);
		
		c.Execute();
		
	}

}
