package eg.edu.alexu.csd.oop.db.cs11;

import java.sql.SQLException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import eg.edu.alexu.csd.oop.db.Database;
import eg.edu.alexu.csd.oop.db.Query;
import eg.edu.alexu.csd.oop.db.Visitor;

public class CreateTable implements Query {

	@Override
	public boolean interpreter(String query) {
		// TODO Auto-generated method stub
		String REGEX = "(\\bcreate table\\b) \\w+(\\()\\w+( \\bint\\b| \\bvarchar\\b)((, \\w+( \\bint\\b| \\bvarchar\\b))+)?\\)";
		Pattern pattern = Pattern.compile(REGEX, Pattern.CASE_INSENSITIVE);
		Matcher matcher = pattern.matcher(query);
		if(matcher.find() && matcher.start() == 0 && matcher.end() == query.length()) {
			return true;
		}
		return false;
	}

	@Override
	public void execute(Database db, String query) throws SQLException {
		// TODO Auto-generated method stub
		boolean success = db.executeStructureQuery(query);
		if (success) {
			String table = query.substring(13, query.indexOf('('));
			System.out.println(table + " successfully created");
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
