package eg.edu.alexu.csd.oop.db.cs11;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.ParserConfigurationException;

import org.xml.sax.SAXException;

import eg.edu.alexu.csd.oop.db.ConditionFilter;
import eg.edu.alexu.csd.oop.db.Database;
import eg.edu.alexu.csd.oop.db.Query;
import eg.edu.alexu.csd.oop.db.Visitor;
import javafx.util.Pair;

public class DBMS implements Database {

	private Files files = new Files();
	private ExecuteVisitor visitor;
	private String currentDatabase;
	private CurrentDatabase current = null;
	private boolean dbCreated = false;

	@Override
	public String createDatabase(String databaseName, boolean dropIfExists) {
		databaseName = databaseName.toLowerCase();
		File file = new File(databaseName);
		if (file.exists() && file.isDirectory()) {
			if (dropIfExists) {
				File[] files = file.listFiles();
				for (int i = 0; i < files.length; i++) {
					files[i].delete();
				}
			}
		} else {
			if (!file.getParentFile().exists()) {
				file.getParentFile().mkdir();
			}
			file.mkdir();
			System.out.println("Database successfully created");
		}
		currentDatabase = file.getAbsolutePath();
		current = CurrentDatabase.getInstance();
		current.setPath(file.getAbsolutePath());
		dbCreated = true;
		try {
			executeStructureQuery("create database " + file.getName());
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return file.getAbsolutePath();
	}

	@Override
	public boolean executeStructureQuery(String query) throws SQLException {
		// TODO Auto-generated method stub
		String exception = query.toLowerCase();
		if (exception.contains("create table") && !exception.contains("(")) {
			throw new SQLException();
		}
		Query createdb = new CreateDatabase();
		Query dropdb = new DropDatabase();
		Query createTable = new CreateTable();
		Query dropTable = new DropTable();
		if (createdb.interpreter(query)) {
			String db = query.substring(16, query.length());
			db = db.toLowerCase();
			File f = new File("sample" + System.getProperty("file.separator") + db);
			if (f.exists()) {
				currentDatabase = f.getAbsolutePath();
				current = CurrentDatabase.getInstance();
				current.setPath(f.getAbsolutePath());
				dbCreated = true;
				System.out.println("Current database is: "+db);
				return true;
			}
		} else if (dropdb.interpreter(query)) {
			String db = query.substring(14, query.length());
			db = db.toLowerCase();
			File f = new File("sample" + System.getProperty("file.separator") + db);
			if (f.exists()) {
				File[] files = f.listFiles();
				for (int i = 0; i < files.length; i++) {
					files[i].delete();
				}
				return true;
			}
		} else if (createTable.interpreter(query)) {
			if(!dbCreated) {
				//return false;
				System.out.println("ERROR!!");
				throw new SQLException();
			}
			if (!current.getPath().equals(null)) {
				HashMap<String, String> map = new HashMap<>();
				String table = query.substring(13, query.indexOf('('));
				File tableFile = new File(currentDatabase + System.getProperty("file.separator") + table + ".xml");
				if (!tableFile.exists()) {
					String column = query.substring(query.indexOf('(') + 1, query.length() - 1);
					String REGEX1 = " \\bint\\b";
					Pattern p1 = Pattern.compile(REGEX1, Pattern.CASE_INSENSITIVE);
					String REGEX2 = " \\bvarchar\\b";
					Pattern p2 = Pattern.compile(REGEX2, Pattern.CASE_INSENSITIVE);
					while (column.contains(",")) {
						String c = column.substring(0, column.indexOf(','));
						Matcher m1 = p1.matcher(c);
						Matcher m2 = p2.matcher(c);
						if (m1.find()) {
							map.put(c.substring(0, m1.start()), "int");
						} else if (m2.find()) {
							map.put(c.substring(0, m2.start()), "varchar");
						}
						column = column.substring(column.indexOf(',') + 2);
					}
					Matcher m1 = p1.matcher(column);
					Matcher m2 = p2.matcher(column);
					if (m1.find()) {
						map.put(column.substring(0, m1.start()), "int");
					} else if (m2.find()) {
						map.put(column.substring(0, m2.start()), "varchar");
					}
					files.createXML(table);
					files.createXSD(table, map);
					return true;
				}
			}
		} else if (dropTable.interpreter(query)) {
			String table = query.substring(11, query.length());
			File xml = new File(currentDatabase + System.getProperty("file.separator") + table + ".xml");
			File xsd = new File(currentDatabase + System.getProperty("file.separator") + table + ".xsd");
			if (xml.exists()) {
				xml.delete();
				if (xsd.exists()) {
					xsd.delete();
				}
				return true;
			}
		} else {
			return false;
		}
		return false;
	}

	@Override
	public Object[][] executeQuery(String query) throws SQLException {
		// TODO Auto-generated method stub
		Query select = new SelectFromTable();
		if (!select.interpreter(query)) {
			throw new SQLException();
		}
		visitor = new ExecuteVisitor();
		visitor.setQuery(query);
		select.accept(visitor);
		if(!visitor.isExecuted()) {
			System.out.println("ERROR!!");
			throw new SQLException();
		}
		return visitor.getArray();
	}

	@Override
	public int executeUpdateQuery(String query) throws SQLException {
		// TODO Auto-generated method stub
		Query delete = new DeleteFromTable();
		Query update = new UpdateTable();
		Query insert = new InsertIntoTable();
		if (delete.interpreter(query)) {
			visitor = new ExecuteVisitor();
			visitor.setQuery(query);
			delete.accept(visitor);
			if (visitor.getChangedRows() == -1) {
				throw new SQLException();
			}
			return visitor.getChangedRows();
			
		} else if (update.interpreter(query)) {
			visitor = new ExecuteVisitor();
			visitor.setQuery(query);
			update.accept(visitor);
			if (visitor.getChangedRows() == -1) {
				throw new SQLException();
			}
			return visitor.getChangedRows();
		} else if (insert.interpreter(query)) {
			visitor = new ExecuteVisitor();
			visitor.setQuery(query);
			insert.accept(visitor);
			if (visitor.getChangedRows() == -1) {
				throw new SQLException();
			}
			//cache.addcommand(visitor.getTable(), visitor.getMap());
			return visitor.getChangedRows();
		} else {
			throw new SQLException();
		}
	}
}
