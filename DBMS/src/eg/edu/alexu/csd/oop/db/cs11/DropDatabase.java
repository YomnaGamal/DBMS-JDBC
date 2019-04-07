package eg.edu.alexu.csd.oop.db.cs11;

import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import eg.edu.alexu.csd.oop.db.Database;
import eg.edu.alexu.csd.oop.db.Query;
import eg.edu.alexu.csd.oop.db.Visitor;

public class DropDatabase implements Query {

	@Override
	public boolean interpreter(String query) {
		// TODO Auto-generated method stub
		String REGEX = "\\bdrop database\\b+( \\w+)";
		Pattern pattern = Pattern.compile(REGEX, Pattern.CASE_INSENSITIVE);
		Matcher matcher = pattern.matcher(query);
		if (matcher.find() && matcher.start() == 0 && matcher.end() == query.length()) {
			return true;
		}
		return false;
	}

	@Override
	public void execute(Database db, String query) throws SQLException {
		// TODO Auto-generated method stub
		boolean success = db.executeStructureQuery(query);
		if (success) {
			String name = query.substring(14, query.length());
			System.out.println(name + " successfully dropped!");
		} else {
			System.out.println("ERROR!!");
		}
	}

	@Override
	public void accept(Visitor visitor) {
		// TODO Auto-generated method stub
		visitor.visit(this);
	}

}
