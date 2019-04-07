package eg.edu.alexu.csd.oop.db.cs11;

import java.util.ArrayList;

import eg.edu.alexu.csd.oop.db.ConditionFilter;

public class ColumnsFilter implements ConditionFilter {

	ArrayList<String> allColumns;
	ArrayList<String> columns;

	public ColumnsFilter(ArrayList<String> allColumns, ArrayList<String> columns) {
		this.columns = columns;
		this.allColumns = allColumns;
	}

	@SuppressWarnings("unchecked")
	@Override
	public ArrayList<ArrayList<Object>> satisfyCondition(ArrayList<ArrayList<Object>> table) {
		// TODO Auto-generated method stub
		ArrayList<ArrayList<Object>> filtered = new ArrayList<ArrayList<Object>>();
		for (int i = 0; i < columns.size(); i++) {
			for (int j = 0; j < allColumns.size(); j++) {
				if (columns.get(i).equalsIgnoreCase(allColumns.get(j))) {
					filtered.add((ArrayList<Object>) table.get(j).clone());
				}
			}
		}
		return filtered;
	}

}
