package eg.edu.alexu.csd.oop.db.cs11;

import java.sql.SQLException;

import eg.edu.alexu.csd.oop.db.Database;
import eg.edu.alexu.csd.oop.db.Query;

public class AccessDatabase {

	Database db;
	Query[] queries;

	public AccessDatabase(Database db, Query[] queries) {
		this.db = db;
		this.queries = queries;
	}

	public void validateSyntax(String query) throws SQLException {
		Parser p = new Parser(queries);
		int index = p.checkSyntax(query);
		if (index > -1) {
			queries[index].execute(db, query);
		} else {
			throw new SQLException();
		}
	}

}
