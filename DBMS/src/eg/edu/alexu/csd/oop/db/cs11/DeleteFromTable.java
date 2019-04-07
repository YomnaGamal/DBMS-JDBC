package eg.edu.alexu.csd.oop.db.cs11;

import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import eg.edu.alexu.csd.oop.db.Database;
import eg.edu.alexu.csd.oop.db.Query;
import eg.edu.alexu.csd.oop.db.Visitor;

public class DeleteFromTable implements Query {

	@Override
	public boolean interpreter(String query) {
		// TODO Auto-generated method stub
		//double space before where
		String REGEX = "\\bdelete from\\b+ \\w+( ( )?\\bwhere\\b+( \\bnot\\b)?"
				+ "( \\w+(=(\\d+|'\\w+')| ((>|<) \\d+)))( (\\band\\b|\\bor\\b)"
				+ "( \\w+(=(\\d+|'\\w+')| ((>|<) \\d+))))?)?";
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
		int count = db.executeUpdateQuery(query);
		System.out.println("Number of changed rows is: "+count);
	}

	@Override
	public void accept(Visitor visitor) {
		// TODO Auto-generated method stub
		visitor.visit(this);
	}

}
