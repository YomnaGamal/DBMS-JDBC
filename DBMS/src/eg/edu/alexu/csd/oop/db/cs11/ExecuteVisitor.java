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
import eg.edu.alexu.csd.oop.db.Visitor;
import javafx.util.Pair;

public class ExecuteVisitor implements Visitor {

	private String query;
	private int changedRows = 0;;
	private Object[][] array;
	private boolean executed;
	private CurrentDatabase current = CurrentDatabase.getInstance();
	private Files files = new Files();
	private String table;
	private HashMap<String, String> map = new HashMap<>();

	public String getTable() {
		return table;
	}

	public HashMap<String, String> getMap() {
		return map;
	}

	public void setQuery(String query) {
		this.query = query;
	}

	public int getChangedRows() {
		return changedRows;
	}

	public Object[][] getArray() {
		return array;
	}

	public boolean isExecuted() {
		return executed;
	}

	@Override
	public void visit(CreateDatabase createdb) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(CreateTable createTable) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(DropDatabase dropdb) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(DropTable dropTabale) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(SelectFromTable select) {
		// TODO Auto-generated method stub
		HashMap<String, String> map = new HashMap<>();
		ArrayList<String> columns = new ArrayList<>();
		ConditionFilter filter = null;
		executed = true;
		String REGEX = " \\bfrom\\b ";
		Pattern pattern = Pattern.compile(REGEX, Pattern.CASE_INSENSITIVE);
		Matcher matcher = pattern.matcher(query);
		if (matcher.find()) {
			String column = query.substring(7, matcher.start());
			String REGEX1 = " \\bwhere\\b ";
			Pattern pattern1 = Pattern.compile(REGEX1, Pattern.CASE_INSENSITIVE);
			Matcher matcher1 = pattern1.matcher(query);
			if (matcher1.find()) {
				table = query.substring(matcher.end(), matcher1.start());
				File tableFile = new File(current.getPath() + System.getProperty("file.separator") + table + ".xml");
				if (!tableFile.exists()) {
					executed = false;
					return;
					// throw new SQLException();
				}
				if (column.equals("*")) {
					map = files.readXSD(table);
					for (Entry<String, String> entry : map.entrySet()) {
						columns.add(entry.getKey());
					}
				} else {
					while (column.contains(",")) {
						columns.add(column.substring(0, column.indexOf(',')));
						column = column.substring(column.indexOf(',') + 2);
					}
					columns.add(column);
				}
				String REGEX2 = " \\band\\b | \\bor\\b ";
				Pattern pattern2 = Pattern.compile(REGEX2, Pattern.CASE_INSENSITIVE);
				Matcher matcher2 = pattern2.matcher(query);
				String REGEX3 = "\\bnot\\b ";
				Pattern pattern3 = Pattern.compile(REGEX3, Pattern.CASE_INSENSITIVE);
				Matcher matcher3 = pattern3.matcher(query);
				if (matcher2.find()) {
					ConditionFilter filter1, filter2;
					String condition1 = query.substring(matcher1.end(), matcher2.start());
					filter1 = filter(condition1, table, columns);
					String condition2 = query.substring(matcher2.end(), query.length());
					filter2 = filter(condition2, table, columns);
					String operator = query.substring(matcher2.start() + 1, matcher2.end() - 1);
					operator = operator.toLowerCase();
					if (operator.equals("and")) {
						filter = new And(filter1, filter2);
					} else if (operator.equals("or")) {
						filter = new Or(filter1, filter2);
					}
				} else if (matcher3.find()) {
					String condition = query.substring(matcher3.end(), query.length());
					filter = new Not(filter(condition, table, columns));
				} else {
					String condition = query.substring(matcher1.end(), query.length());
					filter = filter(condition, table, columns);
				}
			} else {
				table = query.substring(matcher.end(), query.length());
				File tableFile = new File(current.getPath() + System.getProperty("file.separator") + table + ".xml");
				if (!tableFile.exists()) {
					executed = false;
					return;
				}
				if (column.equals("*")) {
					map = files.readXSD(table);
					for (Entry<String, String> entry : map.entrySet()) {
						columns.add(entry.getKey());
					}
				} else {
					while (column.contains(",")) {
						columns.add(column.substring(0, column.indexOf(',')));
						column = column.substring(column.indexOf(',') + 2);
					}
					columns.add(column);
				}
			}
		}
		map = files.readXSD(table);
		int noOfCols = 0;
		for (Entry<String, String> entry : map.entrySet()) {
			for (int i = 0; i < columns.size(); i++) {
				if (entry.getKey().equalsIgnoreCase(columns.get(i))) {
					noOfCols++;
				}
			}
		}
		if (noOfCols != columns.size()) {
			array = null;
			return;
		}
		array = files.selectFromTable(table, columns, filter).clone();
		for (int i = 0; i < columns.size(); i++) {
			System.out.print(columns.get(i) + "   ");
		}
		System.out.println();
		for (int i = 0; i < array.length; i++) {
			for (int j = 0; j < columns.size(); j++) {
				System.out.print(array[i][j] + "            ");
			}
			System.out.println();
		}
	}

	@Override
	public void visit(InsertIntoTable insert) {
		// TODO Auto-generated method stub
		ArrayList<Pair<String, Pair<String, String>>> conditions = new ArrayList<>();
		ArrayList<String> columns = new ArrayList<String>();
		ArrayList<String> values = new ArrayList<String>();
		// 0 for int, 1 for varchar
		HashMap<String, Integer> check = new HashMap<>();
		ArrayList<String> allColumns = new ArrayList<>();
		HashMap<String, String> xsdMap = null;
		String REGEXAll = " \\w+ \\bvalues\\b";
		Pattern pAll = Pattern.compile(REGEXAll, Pattern.CASE_INSENSITIVE);
		Matcher mAll = pAll.matcher(query);
		if (mAll.find()) {
			String REGEXAll2 = " \\bvalues\\b";
			Pattern pAll2 = Pattern.compile(REGEXAll2, Pattern.CASE_INSENSITIVE);
			Matcher mAll2 = pAll2.matcher(query);
			if (mAll2.find()) {
				table = query.substring(12, mAll2.start());
				File tableFile = new File(current.getPath() + System.getProperty("file.separator") + table + ".xml");
				if (!tableFile.exists()) {
					changedRows = -1;
					return;
					// throw new SQLException();
				}
				xsdMap = files.readXSD(table);
				for (String col : xsdMap.keySet()) {
					allColumns.add(col);
				}
				for (int i = 0; i < allColumns.size(); i++) {
					columns.add(allColumns.get(i));
				}
			}
			// add all columns
		} else {
			table = query.substring(12, query.indexOf('('));
			File tableFile = new File(current.getPath() + System.getProperty("file.separator") + table + ".xml");
			if (!tableFile.exists()) {
				changedRows = -1;
				return;
			}
			xsdMap = files.readXSD(table);
			for (String col : xsdMap.keySet()) {
				allColumns.add(col);
			}
			String column = query.substring(query.indexOf('(') + 1, query.indexOf(')'));
			while (column.contains(",")) {
				columns.add(column.substring(0, column.indexOf(',')));
				column = column.substring(column.indexOf(',') + 2);
			}
			columns.add(column);
		}
		String REGEX = " \\bvalues\\b (\\()";
		Pattern pattern = Pattern.compile(REGEX, Pattern.CASE_INSENSITIVE);
		Matcher matcher = pattern.matcher(query);
		if (matcher.find()) {
			String value = query.substring(matcher.end(), query.length() - 1);
			while (value.contains(",")) {
				values.add(value.substring(0, value.indexOf(',')));
				value = value.substring(value.indexOf(',') + 2);
			}
			values.add(value);
			if (columns.size() != values.size()) {
				changedRows = -1;
				return;
				// throw new SQLException();
			}
			for (int i = 0; i < values.size(); i++) {
				String v = values.get(i);
				String k = columns.get(i);
				if (values.get(i).contains("'")) {
					v = v.substring(1, v.length() - 1);
					check.put(k, 1);
				} else {
					check.put(k, 0);
				}
				map.put(k, v);
			}
			int noOfCols = 0;
			for (Entry<String, String> entry : xsdMap.entrySet()) {
				for (int i = 0; i < conditions.size(); i++) {
					if (entry.getKey().equalsIgnoreCase(conditions.get(i).getKey())) {
						noOfCols++;
					}
				}
			}
			if (noOfCols != conditions.size()) {
				changedRows = -1;
				return;
				// throw new SQLException();
			}
			for (Entry<String, Integer> e : check.entrySet()) {
				for (int i = 0; i < allColumns.size(); i++) {
					if (e.getKey().equalsIgnoreCase(allColumns.get(i))) {
						String columnType = xsdMap.get(allColumns.get(i));
						// System.out.println(columnType);
						String[] columnTypeString = columnType.split(":");
						String type = columnTypeString[1];
						if (e.getValue() == 0 && !type.equals("int")) {
							changedRows = 0;
							return;
						} else if (e.getValue() == 1 && !type.equals("varchar")) {
							changedRows = 0;
							return;
						}
					}
				}
			}
			changedRows = files.insertIntoTable(table, map);
			// changedRows = 1;

		}
	}

	@Override
	public void visit(UpdateTable update) {
		// TODO Auto-generated method stub
		HashMap<String, String> map = new HashMap<>();
		// 0 for int, 1 for varchar
		HashMap<String, Integer> check = new HashMap<>();
		ArrayList<Pair<String, Pair<String, String>>> conditions = new ArrayList<>();
		String REGEX = " \\bset\\b ";
		Pattern pattern = Pattern.compile(REGEX, Pattern.CASE_INSENSITIVE);
		Matcher matcher = pattern.matcher(query);
		if (matcher.find()) {
			table = query.substring(7, matcher.start());
			File tableFile = new File(current.getPath() + System.getProperty("file.separator") + table + ".xml");
			if (!tableFile.exists()) {
				changedRows = -1;
				return;
				// throw new SQLException();
			}
			String REGEX1 = " \\bwhere\\b ";
			Pattern pattern1 = Pattern.compile(REGEX1, Pattern.CASE_INSENSITIVE);
			Matcher matcher1 = pattern1.matcher(query);
			if (matcher1.find()) {
				String column = query.substring(matcher.end(), matcher1.start());
				while (column.contains(",")) {
					String c = column.substring(0, column.indexOf(','));
					column = column.substring(column.indexOf(',') + 2, column.length());
					String key = c.substring(0, c.indexOf('='));
					String value = c.substring(c.indexOf('=') + 1, c.length());
					if (value.contains("'")) {
						check.put(key, 1);
						value = value.substring(1, value.length() - 1);
					} else {
						check.put(key, 0);
					}
					map.put(key, value);
				}
				String key = column.substring(0, column.indexOf('='));
				String value = column.substring(column.indexOf('=') + 1, column.length());
				if (value.contains("'")) {
					check.put(key, 1);
					value = value.substring(1, value.length() - 1);
				} else {
					check.put(key, 0);
				}
				// check data type
				HashMap<String, String> xsdMap = files.readXSD(table);
				ArrayList<String> allColumns = new ArrayList<>();
				for (String col : xsdMap.keySet()) {
					allColumns.add(col);
				}
				for (Entry<String, Integer> entry : check.entrySet()) {
					for (int i = 0; i < allColumns.size(); i++) {
						if (entry.getKey().equalsIgnoreCase(allColumns.get(i))) {
							String columnType = xsdMap.get(allColumns.get(i));
							String[] columnTypeString = columnType.split(":");
							String type = columnTypeString[1];
							if (entry.getValue() == 0 && !type.equals("int")) {
								changedRows = 0;
								return;
							} else if (entry.getValue() == 1 && !type.equals("varchar")) {
								changedRows = 0;
								return;
							}
						}
					}

				}
				// map of update
				map.put(key, value);
				String REGEX2 = " \\band\\b | \\bor\\b ";
				Pattern pattern2 = Pattern.compile(REGEX2, Pattern.CASE_INSENSITIVE);
				Matcher matcher2 = pattern2.matcher(query);
				String REGEX3 = "\\bnot\\b ";
				Pattern pattern3 = Pattern.compile(REGEX3, Pattern.CASE_INSENSITIVE);
				Matcher matcher3 = pattern3.matcher(query);
				if (matcher2.find()) {
					String condition1 = query.substring(matcher1.end(), matcher2.start());
					conditions.add(condition(condition1, table)); // return value
					String condition2 = query.substring(matcher2.end(), query.length());
					conditions.add(condition(condition2, table)); // return value
					String operator = query.substring(matcher2.start() + 1, matcher2.end() - 1);
					operator = operator.toLowerCase();
					if(condition(condition1, table)==null||condition(condition2, table)==null) {
						changedRows=-1;
						return;
					}
					int noOfCols = 0;
					for (Entry<String, String> entry : xsdMap.entrySet()) {
						for (int i = 0; i < conditions.size(); i++) {
							if (entry.getKey().equalsIgnoreCase(conditions.get(i).getKey())) {
								noOfCols++;
							}
						}
					}
					if (noOfCols != conditions.size()) {
						changedRows = -1;
						return;
						// throw new SQLException();
					}
					try {
						changedRows += files.updateTable(table, map, conditions, operator);
					} catch (ParserConfigurationException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (SAXException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

				} else if (matcher3.find()) {
					String operator = "not";
					String condition = query.substring(matcher3.end(), query.length());
					conditions.add(condition(condition, table)); // return value
					if(condition(condition, table)==null) {
						changedRows=-1;
						return;
					}
					int noOfCols = 0;
					for (Entry<String, String> entry : xsdMap.entrySet()) {
						for (int i = 0; i < conditions.size(); i++) {
							if (entry.getKey().equalsIgnoreCase(conditions.get(i).getKey())) {
								noOfCols++;
							}
						}
					}
					if (noOfCols != conditions.size()) {
						changedRows = -1;
						return;
						// throw new SQLException();
					}
					try {
						changedRows += files.updateTable(table, map, conditions, operator);
					} catch (ParserConfigurationException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (SAXException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

				} else {
					String operator = "noCondition";
					String condition = query.substring(matcher1.end(), query.length());
					conditions.add(condition(condition, table)); // return value
					if(condition(condition, table)==null) {
						changedRows=-1;
						return;
					}
					int noOfCols = 0;
					for (Entry<String, String> entry : xsdMap.entrySet()) {
						for (int i = 0; i < conditions.size(); i++) {
							if (entry.getKey().equalsIgnoreCase(conditions.get(i).getKey())) {
								noOfCols++;
							}
						}
					}
					if (noOfCols != conditions.size()) {
						changedRows = -1;
						return;
						// throw new SQLException();
					}
					try {
						changedRows = files.updateTable(table, map, conditions, operator);
					} catch (ParserConfigurationException | SAXException | IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			} else {
				// update kol el column
				String operator = "all";
				String column = query.substring(matcher.end(), query.length());
				while (column.contains(",")) {
					String c = column.substring(0, column.indexOf(','));
					column = column.substring(column.indexOf(',') + 2, column.length());
					String key = c.substring(0, c.indexOf('='));
					String value = c.substring(c.indexOf('=') + 1, c.length());
					if (value.contains("'")) {
						check.put(key, 1);
						value = value.substring(1, value.length() - 1);
					} else {
						check.put(key, 0);
					}
					map.put(key, value);
				}
				String key = column.substring(0, column.indexOf('='));
				String value = column.substring(column.indexOf('=') + 1, column.length());
				if (value.contains("'")) {
					check.put(key, 1);
					value = value.substring(1, value.length() - 1);
				} else {
					check.put(key, 0);
				}
				// map of update
				map.put(key, value);
				HashMap<String, String> xsdMap = files.readXSD(table);
				int noOfCols = 0;
				for (Entry<String, String> entry : xsdMap.entrySet()) {
					for (int i = 0; i < conditions.size(); i++) {
						if (entry.getKey().equalsIgnoreCase(conditions.get(i).getKey())) {
							noOfCols++;
						}
					}
				}
				if (noOfCols != conditions.size()) {
					changedRows = -1;
					return;
					// throw new SQLException();
				}
				try {
					changedRows = files.updateTable(table, map, conditions, operator);
				} catch (ParserConfigurationException | SAXException | IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

	@Override
	public void visit(DeleteFromTable delete) {
		// TODO Auto-generated method stub
		ArrayList<Pair<String, Pair<String, String>>> conditions = new ArrayList<>();
		String REGEX = "( )? \\bwhere\\b ";
		Pattern pattern = Pattern.compile(REGEX, Pattern.CASE_INSENSITIVE);
		Matcher matcher = pattern.matcher(query);
		if (matcher.find()) {
			table = query.substring(12, matcher.start());
			File tableFile = new File(current.getPath() + System.getProperty("file.separator") + table + ".xml");
			if (!tableFile.exists()) {
				changedRows = -1;
				return;
				// throw new SQLException();
			}
			String REGEX2 = " \\band\\b | \\bor\\b ";
			Pattern pattern2 = Pattern.compile(REGEX2, Pattern.CASE_INSENSITIVE);
			Matcher matcher2 = pattern2.matcher(query);
			String REGEX3 = "\\bnot\\b ";
			Pattern pattern3 = Pattern.compile(REGEX3, Pattern.CASE_INSENSITIVE);
			Matcher matcher3 = pattern3.matcher(query);
			if (matcher2.find()) {
				String condition1 = query.substring(matcher.end(), matcher2.start());
				conditions.add(condition(condition1, table)); // return value
				String condition2 = query.substring(matcher2.end(), query.length());
				conditions.add(condition(condition2, table)); // return value
				String operator = query.substring(matcher2.start() + 1, matcher2.end() - 1);
				operator = operator.toLowerCase();
				// perform delete
				HashMap<String, String> xsdMap = files.readXSD(table);
				int noOfCols = 0;
				if(condition(condition1, table)==null||condition(condition2, table)==null) {
					changedRows=-1;
					return;
				}
				for (Entry<String, String> entry : xsdMap.entrySet()) {
					for (int i = 0; i < conditions.size(); i++) {
						if (entry.getKey().equalsIgnoreCase(conditions.get(i).getKey())) {
							noOfCols++;
						}
					}
				}
				if (noOfCols != conditions.size()) {
					changedRows = -1;
					return;
					// throw new SQLException();
				}
				try {
					changedRows = files.deleteFromTable(table, conditions, operator);
				} catch (ParserConfigurationException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (SAXException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			} else if (matcher3.find()) {
				String operator = "not";
				String condition = query.substring(matcher3.end(), query.length());
				conditions.add(condition(condition, table)); // return value
				HashMap<String, String> xsdMap = files.readXSD(table);
				if(condition(condition, table)==null) {
					changedRows=-1;
					return;
				}
				int noOfCols = 0;
				for (Entry<String, String> entry : xsdMap.entrySet()) {
					for (int i = 0; i < conditions.size(); i++) {
						if (entry.getKey().equalsIgnoreCase(conditions.get(i).getKey())) {
							noOfCols++;
						}
					}
				}
				if (noOfCols != conditions.size()) {
					changedRows = -1;
					return;
					// throw new SQLException();
				}
				try {
					changedRows = files.deleteFromTable(table, conditions, operator);
				} catch (ParserConfigurationException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (SAXException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				// perform delete
			} else {
				// without AND,OR, NOT
				String operator = "noCondition";
				String condition = query.substring(matcher.end(), query.length());
				conditions.add(condition(condition, table)); // return value
				if(condition(condition, table)==null) {
					changedRows=-1;
					return;
				}
				HashMap<String, String> xsdMap = files.readXSD(table);
				int noOfCols = 0;
				for (Entry<String, String> entry : xsdMap.entrySet()) {
					for (int i = 0; i < conditions.size(); i++) {
						if (entry.getKey().equalsIgnoreCase(conditions.get(i).getKey())) {
							noOfCols++;
						}
					}
				}
				if (noOfCols != conditions.size()) {
					changedRows = -1;
					return;
					// throw new SQLException();
				}
				try {
					changedRows = files.deleteFromTable(table, conditions, operator);
				} catch (ParserConfigurationException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (SAXException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				// perform delete
			}
		} else {
			String operator = "all";
			table = query.substring(12, query.length());
			// String condition = query.substring(matcher.end(), query.length());
			HashMap<String, String> xsdMap = files.readXSD(table);
			int noOfCols = 0;
			for (Entry<String, String> entry : xsdMap.entrySet()) {
				for (int i = 0; i < conditions.size(); i++) {
					if (entry.getKey().equalsIgnoreCase(conditions.get(i).getKey())) {
						noOfCols++;
					}
				}
			}
			if (noOfCols != conditions.size()) {
				changedRows = -1;
				return;
				// throw new SQLException();
			}
			try {
				changedRows = files.deleteFromTable(table, conditions, operator);
			} catch (ParserConfigurationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SAXException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			// clear table
		}
	}

	private Pair<String, Pair<String, String>> condition(String condition, String table) {
		// char operator;
		String character;
		String column, value;
		boolean exists = false;
		Pair<String, Pair<String, String>> conditionMap = null;
		HashMap<String, String> map = files.readXSD(table);
		ArrayList<String> allColumns = new ArrayList<>();
		for (String col : map.keySet()) {
			allColumns.add(col);
		}
		if (condition.contains("=")) {
			column = condition.substring(0, condition.indexOf('='));
			value = condition.substring(condition.indexOf('=') + 1, condition.length());
			for (String col : map.keySet()) {
				if (col.equalsIgnoreCase(column)) {
					exists = true;
				}
			}
			if (!exists) {
				try {
					throw new SQLException();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else {
				if (value.contains("'")) {
					value = value.substring(1, value.length() - 1);
					// check map
					for (int i = 0; i < allColumns.size(); i++) {
						if (column.equalsIgnoreCase(allColumns.get(i))) {
							String columnType = map.get(allColumns.get(i));
							String[] columnTypeString = columnType.split(":");
							String type = columnTypeString[1];
							if (type.equals("varchar")) {
								character = "==";
								Pair<String, String> pair = new Pair(value, character);
								conditionMap = new Pair(column, pair);
							} else if (type.equals("int")) {
								try {
									throw new SQLException();
								} catch (SQLException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
							}
						}
					}
				} else {
					for (int i = 0; i < allColumns.size(); i++) {
						if (column.equalsIgnoreCase(allColumns.get(i))) {
							String columnType = map.get(allColumns.get(i));
							String[] columnTypeString = columnType.split(":");
							String type = columnTypeString[1];
							if (type.equals("varchar")) {
								try {
									throw new SQLException();
								} catch (SQLException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
							} else if (type.equals("int")) {
								character = "=";
								Pair<String, String> pair = new Pair<>(value, character);
								conditionMap = new Pair(column, pair);
							}
						}
					}
				}
			}
		} else if (condition.contains(">")) {
			column = condition.substring(0, condition.indexOf('>') - 1);
			value = condition.substring(condition.indexOf('>') + 2, condition.length());
			for (String col : map.keySet()) {
				if (col.equalsIgnoreCase(column)) {
					exists = true;
				}
			}
			if (!exists) {
				try {
					throw new SQLException();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else {
				for (int i = 0; i < allColumns.size(); i++) {
					if (column.equalsIgnoreCase(allColumns.get(i))) {
						String columnType = map.get(allColumns.get(i));
						String[] columnTypeString = columnType.split(":");
						String type = columnTypeString[1];
						if (type.equals("varchar")) {
							try {
								throw new SQLException();
							} catch (SQLException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						} else if (type.equals("int")) {
							character = ">";
							Pair pair = new Pair(value, character);
							conditionMap = new Pair(column, pair);
						}
					}
				}

			}
		} else if (condition.contains("<")) {
			column = condition.substring(0, condition.indexOf('<') - 1);
			value = condition.substring(condition.indexOf('<') + 2, condition.length());
			for (String col : map.keySet()) {
				if (col.equalsIgnoreCase(column)) {
					exists = true;
				}
			}
			if (!exists) {
				try {
					throw new SQLException();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else {
				for (int i = 0; i < allColumns.size(); i++) {
					if (column.equalsIgnoreCase(allColumns.get(i))) {
						String columnType = map.get(allColumns.get(i));
						String[] columnTypeString = columnType.split(":");
						String type = columnTypeString[1];
						if (type.equals("varchar")) {
							try {
								throw new SQLException();
							} catch (SQLException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						} else if (type.equals("int")) {
							character = "<";
							Pair pair = new Pair(value, character);
							conditionMap = new Pair(column, pair);
						}
					}
				}
			}
		}
		return conditionMap;
	}

	private ConditionFilter filter(String condition, String table, ArrayList<String> columns) {
		ConditionFilter filter = null;
		Pair<String, Pair<String, String>> c = condition(condition, table);
		if (c == null) {
			try {
				throw new SQLException();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			String character = c.getValue().getValue();
			HashMap<String, String> xsdMap = files.readXSD(table);
			ArrayList<String> allColumns = new ArrayList<>();
			for (String col : xsdMap.keySet()) {
				allColumns.add(col);
			}
			String key = c.getKey();
			String type = "";
			for (Entry<String, String> entry : xsdMap.entrySet()) {
				if (key.equalsIgnoreCase(entry.getKey())) {
					type = entry.getValue();
				}
			}
			if (character.equals("=")) {
				Object reference = null;
				reference = Integer.parseInt(c.getValue().getKey());
				filter = new EqualityFilter(reference, allColumns, columns, key);
			} else if (character.equals(">")) {
				Integer reference = Integer.parseInt(c.getValue().getKey());
				filter = new GreaterThanFilter(reference, allColumns, columns, key);
			} else if (character.equals("<")) {
				Integer reference = Integer.parseInt(c.getValue().getKey());
				filter = new SmallerThanFilter(reference, allColumns, columns, key);
			} else if (character.equals("==")) {
				Object reference = null;
				reference = c.getValue().getKey();
				filter = new EqualityFilter(reference, allColumns, columns, key);
			}
		}
		return filter;
	}
}
