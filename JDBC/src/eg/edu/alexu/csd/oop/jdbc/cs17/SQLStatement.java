package eg.edu.alexu.csd.oop.jdbc.cs17;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import eg.edu.alexu.csd.oop.db.Database;
import eg.edu.alexu.csd.oop.db.cs11.DBMS;

public class SQLStatement implements Statement{

	private ArrayList<String> commands=new ArrayList<>();
	private Connection connection;
	private Database dbms;
	private ResultSet result;
	private int count;
	private boolean closed;
	private int timeOut = 0;
	private Log log=new Log();

	public SQLStatement(Connection con, Database dbms) {
		this.connection=con;
		this.closed = false;
		this.dbms = dbms;
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		// TODO Auto-generated method stub
		throw new java.lang.UnsupportedOperationException();
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		// TODO Auto-generated method stub
		throw new java.lang.UnsupportedOperationException();
	}

	@Override
	public void addBatch(String sql) throws SQLException {
		if (closed) {
			log.getLogger().warning("Can not use closed statement!");
			throw new SQLException();
		}
		commands.add(sql);	
		log.getLogger().info("Batch added successfully!");
	}

	@Override
	public void cancel() throws SQLException {
		// TODO Auto-generated method stub
		throw new java.lang.UnsupportedOperationException();
	}

	@Override
	public void clearBatch() throws SQLException {
		if (closed) {
			log.getLogger().warning("Can not use closed statement!");
			throw new SQLException();
		}
		commands.clear();	
		log.getLogger().info("Batch cleared successfully!");
	}

	@Override
	public void clearWarnings() throws SQLException {
		// TODO Auto-generated method stub
		throw new java.lang.UnsupportedOperationException();
	}

	@Override
	public void close() throws SQLException {
		// TODO Auto-generated method stub
		log.getLogger().info("Statement is closed successfully!");
		closed = true;
	}

	@Override
	public void closeOnCompletion() throws SQLException {
		// TODO Auto-generated method stub
		throw new java.lang.UnsupportedOperationException();
	}

	@Override
	public boolean execute(String sql) throws SQLException {
		// TODO Auto-generated method stub
		if (closed) {
			log.getLogger().warning("Can not use closed statement!");
			throw new SQLException();
		}
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
			//try {
				boolean b = dbms.executeStructureQuery(sql);
				log.getLogger().info("Query is successfully executed!");
				return b;
			//} catch (SQLException e) {
				// TODO Auto-generated catch block
			//	log.getLogger().warning("SQL error!");
			//	e.printStackTrace();
			//}	
		}
		REGEX = "\\bselect\\b";
		pattern = Pattern.compile(REGEX, Pattern.CASE_INSENSITIVE);
		matcher = pattern.matcher(sql);
		if(matcher.find()) {
			try {
				Object[][] table = dbms.executeQuery(sql);
				if (table != null && table.length != 0) {
					ResultSetMetaData metaData = new SQLResultSetMetaData(dbms.getTable(), dbms);
					result = new SQLResultSet(this, metaData, table);
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
		/*ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<Boolean> future = executor.submit(new Task(dbms, sql, this));
        boolean x = false;
        try {
            x = future.get(500, TimeUnit.SECONDS);
        } catch (Exception e) {
            future.cancel(true);
        }
        executor.shutdownNow();
        return x;*/
	}

	@Override
	public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
		// TODO Auto-generated method stub
		throw new java.lang.UnsupportedOperationException();
	}

	@Override
	public boolean execute(String sql, int[] columnIndexes) throws SQLException {
		// TODO Auto-generated method stub
		throw new java.lang.UnsupportedOperationException();
	}

	@Override
	public boolean execute(String sql, String[] columnNames) throws SQLException {
		// TODO Auto-generated method stub
		throw new java.lang.UnsupportedOperationException();
	}

	@Override
	public int[] executeBatch() throws SQLException {
		// TODO Auto-generated method stub
		if (closed) {
			log.getLogger().warning("Can not use closed statement!");
			throw new SQLException();
		}
		int[] counts = new int[commands.size()];
		for (int i = 0; i < commands.size(); i++) {
			try {
				execute(commands.get(i));
				counts[i] = getUpdateCount();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				log.getLogger().warning("SQL error!");
				e.printStackTrace();
			}
		}
		log.getLogger().info("Batch is successfully executed!");
		return counts;
	}

	@Override
	public ResultSet executeQuery(String sql) throws SQLException {
		// TODO Auto-generated method stub
		if(closed) {
			log.getLogger().warning("Can not use closed statement!");
			throw new SQLException();
		}
		try {
			Object[][] table = dbms.executeQuery(sql);
			if (table != null) {
				ResultSetMetaData metaData = new SQLResultSetMetaData(dbms.getTable(), dbms);
				result = new SQLResultSet(this, metaData, table);
				log.getLogger().info("Query is successfully executed!");
				return result;
			} else {
				log.getLogger().warning("Null table!");
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public int executeUpdate(String sql) throws SQLException {
		// TODO Auto-generated method stub
		if (closed) {
			log.getLogger().warning("Can not use closed statement!");
			throw new SQLException();
		}
		log.getLogger().info("Query is successfully executed!");
		return dbms.executeUpdateQuery(sql);
	}

	@Override
	public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
		// TODO Auto-generated method stub
		throw new java.lang.UnsupportedOperationException();
	}

	@Override
	public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
		// TODO Auto-generated method stub
		throw new java.lang.UnsupportedOperationException();
	}

	@Override
	public int executeUpdate(String sql, String[] columnNames) throws SQLException {
		// TODO Auto-generated method stub
		throw new java.lang.UnsupportedOperationException();
	}

	@Override
	public Connection getConnection() throws SQLException {
		// TODO Auto-generated method stub
		if (closed) {
			log.getLogger().warning("Can not use closed statement!");
			throw new SQLException();
		}
		log.getLogger().info("Connection successfully returned!");
		return connection;
	}

	@Override
	public int getFetchDirection() throws SQLException {
		// TODO Auto-generated method stub
		throw new java.lang.UnsupportedOperationException();
	}

	@Override
	public int getFetchSize() throws SQLException {
		// TODO Auto-generated method stub
		throw new java.lang.UnsupportedOperationException();
	}

	@Override
	public ResultSet getGeneratedKeys() throws SQLException {
		// TODO Auto-generated method stub
		throw new java.lang.UnsupportedOperationException();
	}

	@Override
	public int getMaxFieldSize() throws SQLException {
		// TODO Auto-generated method stub
		throw new java.lang.UnsupportedOperationException();
	}

	@Override
	public int getMaxRows() throws SQLException {
		// TODO Auto-generated method stub
		throw new java.lang.UnsupportedOperationException();
	}

	@Override
	public boolean getMoreResults() throws SQLException {
		// TODO Auto-generated method stub
		throw new java.lang.UnsupportedOperationException();
	}

	@Override
	public boolean getMoreResults(int current) throws SQLException {
		// TODO Auto-generated method stub
		throw new java.lang.UnsupportedOperationException();
	}

	@Override
	public int getQueryTimeout() throws SQLException {
		// TODO Auto-generated method stub
		if (closed) {
			log.getLogger().warning("Can not use closed statement!");
			throw new SQLException();
		}
		log.getLogger().info("Timeout successfully returned!");
		return timeOut;
	}

	@Override
	public ResultSet getResultSet() throws SQLException {
		// TODO Auto-generated method stub
		return result;
	}

	@Override
	public int getResultSetConcurrency() throws SQLException {
		// TODO Auto-generated method stub
		throw new java.lang.UnsupportedOperationException();
	}

	@Override
	public int getResultSetHoldability() throws SQLException {
		// TODO Auto-generated method stub
		throw new java.lang.UnsupportedOperationException();
	}

	@Override
	public int getResultSetType() throws SQLException {
		// TODO Auto-generated method stub
		throw new java.lang.UnsupportedOperationException();
	}

	@Override
	public int getUpdateCount() throws SQLException {
		// TODO Auto-generated method stub
		if (closed) {
			log.getLogger().warning("Can not use closed statement!");
			throw new SQLException();
		}
		log.getLogger().info("Update count successfully returned!");
		return count;
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		// TODO Auto-generated method stub
		throw new java.lang.UnsupportedOperationException();
	}

	@Override
	public boolean isCloseOnCompletion() throws SQLException {
		// TODO Auto-generated method stub
		throw new java.lang.UnsupportedOperationException();
	}

	@Override
	public boolean isClosed() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isPoolable() throws SQLException {
		// TODO Auto-generated method stub
		throw new java.lang.UnsupportedOperationException();
	}

	@Override
	public void setCursorName(String name) throws SQLException {
		// TODO Auto-generated method stub
		throw new java.lang.UnsupportedOperationException();
	}

	@Override
	public void setEscapeProcessing(boolean enable) throws SQLException {
		// TODO Auto-generated method stub
		throw new java.lang.UnsupportedOperationException();
	}

	@Override
	public void setFetchDirection(int direction) throws SQLException {
		// TODO Auto-generated method stub
		throw new java.lang.UnsupportedOperationException();
	}

	@Override
	public void setFetchSize(int rows) throws SQLException {
		// TODO Auto-generated method stub
		throw new java.lang.UnsupportedOperationException();
	}

	@Override
	public void setMaxFieldSize(int max) throws SQLException {
		// TODO Auto-generated method stub
		throw new java.lang.UnsupportedOperationException();
	}

	@Override
	public void setMaxRows(int max) throws SQLException {
		// TODO Auto-generated method stub
		throw new java.lang.UnsupportedOperationException();
	}

	@Override
	public void setPoolable(boolean poolable) throws SQLException {
		// TODO Auto-generated method stub
		throw new java.lang.UnsupportedOperationException();
	}

	@Override
	public void setQueryTimeout(int seconds) throws SQLException {
		// TODO Auto-generated method stub
		if (closed) {
			log.getLogger().warning("Can not use closed statement!");
			throw new SQLException();
		}
		timeOut = seconds;
		log.getLogger().info("Timeout is successfully set!");
	}
}
