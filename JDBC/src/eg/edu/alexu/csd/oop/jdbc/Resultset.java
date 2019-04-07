package eg.edu.alexu.csd.oop.jdbc;

import java.sql.SQLException;

public interface Resultset {

	/**
	 * Moves the cursor to the given row number in this ResultSet object.
	 **/
	public boolean absolute(int row) throws SQLException;

	/**
	 * Moves the cursor to the end of this ResultSet object, just after the last
	 * row.
	 **/
	public void afterLast() throws SQLException;

	/**
	 * Moves the cursor to the front of this ResultSet object, just before the first
	 * row.
	 **/
	public void beforeFirst() throws SQLException;

	/**
	 * Releases this ResultSet object's database and JDBC resources immediately
	 * instead of waiting for this to happen when it is automatically closed.
	 **/
	public void close() throws SQLException;

	/**
	 * Maps the given ResultSet column label to its ResultSet column index.
	 **/
	public int findColumn(String columnLabel) throws SQLException;

	/**
	 * Moves the cursor to the first row in this ResultSet object.
	 **/
	public boolean first() throws SQLException;

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * ResultSet object as an int in the Java programming language.
	 **/
	public int getInt(int columnIndex) throws SQLException;

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * ResultSet object as an long in the Java programming language.
	 **/
	public long getInt(String columnLabel) throws SQLException;

	/**
	 * Retrieves the number, types and properties of this ResultSet object's
	 * columns.
	 **/
	public ResultSetMetaData getMetaData() throws SQLException;

	/**
	 * Gets the value of the designated column in the current row of this ResultSet
	 * object as an Object in the Java programming language.
	 **/
	public Object getObject(int columnIndex) throws SQLException;

	/**
	 * Retrieves the Statement object that produced this ResultSet object.
	 **/
	public Statement getStatement() throws SQLException;

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * ResultSet object as a String in the Java programming language.
	 **/
	public String getString(int columnIndex) throws SQLException;

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * ResultSet object as a String in the Java programming language.
	 **/
	public String getString(String columnLabel) throws SQLException;

	/**
	 * Retrieves whether the cursor is after the last row in this ResultSet object.
	 **/
	public boolean isAfterLast() throws SQLException;

	/**
	 * Retrieves whether the cursor is before the first row in this ResultSet
	 * object.
	 **/
	public boolean isBeforeFirst() throws SQLException;

	/**
	 * Retrieves whether this ResultSet object has been closed.
	 **/
	public boolean isClosed() throws SQLException;

	/**
	 * Retrieves whether the cursor is on the first row of this ResultSet object.
	 **/
	public boolean isFirst() throws SQLException;

	/**
	 * Retrieves whether the cursor is on the last row of this ResultSet object.
	 **/
	public boolean isLast() throws SQLException;

	/**
	 * Moves the cursor to the last row in this ResultSet object.
	 **/
	public boolean last() throws SQLException;

	/**
	 * Moves the cursor forward one row from its current position.
	 **/
	public boolean next() throws SQLException;

	/**
	 * Moves the cursor to the previous row in this ResultSet object.
	 **/
	public boolean previous() throws SQLException;
}
