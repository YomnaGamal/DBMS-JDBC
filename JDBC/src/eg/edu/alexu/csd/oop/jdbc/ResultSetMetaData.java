package eg.edu.alexu.csd.oop.jdbc;

import java.sql.SQLException;

public interface ResultSetMetaData {

	/**
	 * Returns the number of columns in this ResultSet object.
	 **/
	public int getColumnCount() throws SQLException;

	/**
	 * Gets the designated column's suggested title for use in printouts and
	 * displays.
	 **/
	public String getColumnLabel(int column) throws SQLException;

	/**
	 * Get the designated column's name.
	 **/
	public String getColumnName(int column) throws SQLException;

	/**
	 * Retrieves the designated column's SQL type.
	 **/
	public int getColumnType(int column) throws SQLException;

	/**
	 * Gets the designated column's table name.
	 **/
	public String getTableName(int column) throws SQLException;
}
