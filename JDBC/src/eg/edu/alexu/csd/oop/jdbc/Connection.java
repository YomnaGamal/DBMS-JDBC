package eg.edu.alexu.csd.oop.jdbc;

import java.sql.SQLException;
 
public interface Connection {

	/**
	 * Creates a Statement object for sending SQL statements to the database. SQL
	 * statements without parameters are normally executed using Statement objects.
	 * If the same SQL statement is executed many times, it may be more efficient to
	 * use a PreparedStatement object. Result sets created using the returned
	 * Statement object will by default be type TYPE_FORWARD_ONLY and have a
	 * concurrency level of CONCUR_READ_ONLY. The holdability of the created result
	 * sets can be determined by calling getHoldability().
	 **/

	 public Statement createStatement() throws SQLException;

	/**
	 * Releases this Connection object's database and JDBC resources immediately
	 * instead of waiting for them to be automatically released. Calling the method
	 * close on a Connection object that is already closed is a no-op.
	 * 
	 * It is strongly recommended that an application explicitly commits or rolls
	 * back an active transaction prior to calling the close method. If the close
	 * method is called and there is an active transaction, the results are
	 * implementation-defined.
	 **/

	public void close() throws SQLException;
}
