package eg.edu.alexu.csd.oop.db;

import java.sql.SQLException;

public interface Query {

	public boolean interpreter(String query);
	public void execute(Database db, String query) throws SQLException;
	public void accept(Visitor visitor);
}
