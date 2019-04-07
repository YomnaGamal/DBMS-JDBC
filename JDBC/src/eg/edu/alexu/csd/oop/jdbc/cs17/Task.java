package eg.edu.alexu.csd.oop.jdbc.cs17;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import eg.edu.alexu.csd.oop.db.Database;

class Task implements Callable<Boolean> {

	private String sql;
	private Database dbms;
	private Log log=new Log();
	private int count;
	private ResultSet result;
	private Statement st;
	
	public Task (Database dbms, String sql, Statement st) {
    	this.sql = sql;
    	this.dbms = dbms;
    	this.st = st;
    }

    public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}
	public ResultSet getResult() {
		return result;
	}
	public void setResult(ResultSet result) {
		this.result = result;
	}
	
    @Override
    public Boolean call() throws Exception {
		count = 0;
		String REGEX = "\\bcreate database\\b+( \\w+)";
		Pattern pattern = Pattern.compile(REGEX, Pattern.CASE_INSENSITIVE);
		Matcher matcher = pattern.matcher(sql);
		if (matcher.find() && matcher.start() == 0 && matcher.end() == sql.length()) {
			String name = sql.substring(16, sql.length());
			dbms.createDatabase("sample" + System.getProperty("file.separator") + name, false);
			return true;
		}
		REGEX = "(\\bcreate\\b)|(\\bdrop\\b)";
		pattern = Pattern.compile(REGEX, Pattern.CASE_INSENSITIVE);
		matcher = pattern.matcher(sql);
		if(matcher.find()) {
			try {
				boolean b = dbms.executeStructureQuery(sql);
				log.getLogger().info("Query is successfully executed!");
				return b;
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				log.getLogger().warning("SQL error!");
				e.printStackTrace();
			}	
		}
		REGEX = "\\bselect\\b";
		pattern = Pattern.compile(REGEX, Pattern.CASE_INSENSITIVE);
		matcher = pattern.matcher(sql);
		if(matcher.find()) {
			try {
				Object[][] table = dbms.executeQuery(sql);
				if (table != null && table.length != 0) {
					ResultSetMetaData metaData = new SQLResultSetMetaData(dbms.getTable(), dbms);
					result = new SQLResultSet(st, metaData, table);
					log.getLogger().info("Query is successfully executed!");
					return true;
				} else if (table.length == 0) {
					log.getLogger().info("Query is successfully executed!");
				} else {
					log.getLogger().warning("Null table!");
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				log.getLogger().warning("SQL error!");
				e.printStackTrace();
			}
		}
		REGEX = "(\\bupdate\\b)|(\\binsert\\b)|(\\bdelete\\b)";
		pattern = Pattern.compile(REGEX, Pattern.CASE_INSENSITIVE);
		matcher = pattern.matcher(sql);
		if(matcher.find()) {
			try {
				count = dbms.executeUpdateQuery(sql);
				if (count > 0) {
					log.getLogger().info("Query is successfully executed!");
					return true;
				}
				log.getLogger().info("Query is successfully executed!");
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				log.getLogger().warning("SQL error!");
				e.printStackTrace();
			}
		}
        return false;
    }
}
