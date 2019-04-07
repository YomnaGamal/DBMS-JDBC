package eg.edu.alexu.csd.oop.jdbc;

import java.sql.SQLException;

public interface Statement {

	/**
	 * Adds the given SQL command to the current list of commands for this Statement
	 * object. The commands in this list can be executed as a batch by calling the
	 * method executeBatch. Note:This method cannot be called on a PreparedStatement
	 * or CallableStatement.
	 * 
	 **/
	public void addBatch(String sql) throws SQLException;

	/**
	 * Empties this Statement object's current list of SQL commands.
	 **/
	public void clearBatch() throws SQLException;

	/**
	 * Releases this Statement object's database and JDBC resources immediately
	 * instead of waiting for this to happen when it is automatically closed. 
	 * Note:When a Statement object is closed, its current ResultSet object, if one
	 * exists, is also closed.
	 **/
	public void close() throws SQLException;

	/**
	 * Executes the given SQL statement, which may return multiple results. In some
	 * (uncommon) situations, a single SQL statement may return multiple result sets
	 * and/or update counts. Normally you can ignore this unless you are (1)
	 * executing a stored procedure that you know may return multiple results or (2)
	 * you are dynamically executing an unknown SQL string. The execute method
	 * executes an SQL statement and indicates the form of the first result. You
	 * must then use the methods getResultSet or getUpdateCount to retrieve the
	 * result, and getMoreResults to move to any subsequent result(s).
	 * 
	 * Note:This method cannot be called on a PreparedStatement or
	 * CallableStatement.
	 **/
	public boolean execute(String sql) throws SQLException;

	/**
	 * Submits a batch of commands to the database for execution and if all commands
	 * execute successfully, returns an array of update counts.
	 **/
	public int[] executeBatch() throws SQLException;

	/**
	 * Executes the given SQL statement, which returns a single ResultSet object.
	 **/
	public Resultset executeQuery(String sql) throws SQLException;

	/**
	 * Executes the given SQL statement, which may be an INSERT, UPDATE, or DELETE
	 * statement or an SQL statement that returns nothing, such as an SQL DDL
	 * statement.
	 **/
	public int executeUpdate(String sql) throws SQLException;

	/**
	 * Retrieves the Connection object that produced this Statement object.
	 **/
	public Connection getConnection() throws SQLException;

	/**
	 * Retrieves the number of seconds the driver will wait for a Statement object
	 * to execute.
	 **/
	public int getQueryTimeout() throws SQLException;

	/**
	 * Sets the number of seconds the driver will wait for a Statement object to
	 * execute to the given number of seconds.
	 **/
	public void setQueryTimeout(int seconds) throws SQLException;
}
