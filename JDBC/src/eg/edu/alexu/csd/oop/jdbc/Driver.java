package eg.edu.alexu.csd.oop.jdbc;

import java.sql.SQLException;
import java.util.Properties;

public interface Driver {

	/**
	 * Retrieves whether the driver thinks that it can open a connection to the
	 * given URL. Typically drivers will return true if they understand the
	 * sub-protocol specified in the URL and false if they do not.
	 **/
	public boolean acceptsURL(String url) throws SQLException;

	/**
	 * Attempts to make a database connection to the given URL. The driver should
	 * return "null" if it realizes it is the wrong kind of driver to connect to the
	 * given URL. This will be common, as when the JDBC driver manager is asked to
	 * connect to a given URL it passes the URL to each loaded driver in turn. The
	 * driver should throw an SQLException if it is the right driver to connect to
	 * the given URL but has trouble connecting to the database.
	 * 
	 * The Properties argument can be used to pass arbitrary string tag/value pairs
	 * as connection arguments. Normally at least "user" and "password" properties
	 * should be included in the Properties object.
	 * 
	 * Note: If a property is specified as part of the url and is also specified in
	 * the Properties object, it is implementation-defined as to which value will
	 * take precedence. For maximum portability, an application should only specify
	 * a property once.
	 * 
	 **/

	 public Connection connect(String url, Properties info) throws SQLException;

	/**
	 * Gets information about the possible properties for this driver. The
	 * getPropertyInfo method is intended to allow a generic GUI tool to discover
	 * what properties it should prompt a human for in order to get enough
	 * information to connect to a database. Note that depending on the values the
	 * human has supplied so far, additional values may become necessary, so it may
	 * be necessary to iterate though several calls to the getPropertyInfo method.
	 * 
	 **/
	 //public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException;

}
