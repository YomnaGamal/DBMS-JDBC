package eg.edu.alexu.csd.oop.db.cs11;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Assert;

import eg.edu.alexu.csd.oop.db.Database;

public class Trial {

	public static void main(String[] args) throws SQLException {
		// TODO Auto-generated method stub
		String query = "UPDATE table_name1 SET column_name1='11111111', COLUMN_NAME2=10, column_name3='333333333' WHERE not coLUmn_NAME2=5";
		String REGEX = "\\bupdate\\b \\w+ \\bset\\b \\w+(=)(('\\w+')|\\d+)((, \\w+(=)(('\\w+')|\\d+))+)?"
				+ "( \\bwhere\\b( \\bnot)? \\w+((=(\\d+|'\\w+'))| ((>|<) \\d+))( (\\band\\b|\\bor\\b)"
				+ "( \\w+(=(\\d+|'\\w+')| ((>|<) \\d+))))?)?";
		Pattern pattern = Pattern.compile(REGEX, Pattern.CASE_INSENSITIVE);
		Matcher matcher = pattern.matcher(query);
		if (matcher.find() && matcher.start() == 0 && matcher.end() == query.length()) {
			System.out.println(query);
		}
	}

}
