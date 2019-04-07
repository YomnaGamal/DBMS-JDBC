package eg.edu.alexu.csd.oop.db.cs11;

import java.util.ArrayList;

import eg.edu.alexu.csd.oop.db.ConditionFilter;

public class SmallerThanFilter implements ConditionFilter {

	private Object reference;
	private ArrayList<String> columns;
	private String column;
	private ArrayList<String> allColumns;

	public SmallerThanFilter(Object reference, ArrayList<String> allColumns, ArrayList<String> columns, String column) {
		this.reference = reference;
		this.columns = columns;
		this.column = column;
		this.allColumns = allColumns;
	}

	@SuppressWarnings("unchecked")
	@Override
	public ArrayList<ArrayList<Object>> satisfyCondition(ArrayList<ArrayList<Object>> table) {
		// TODO Auto-generated method stub
		ArrayList<ArrayList<Object>> copy = new ArrayList<ArrayList<Object>>();
		//copy = (ArrayList<ArrayList<Object>>) table.clone();
		for (int i = 0; i < table.size(); i++) {
			ArrayList<Object> c = new ArrayList<Object>();
			for(int j = 0; j < table.get(i).size(); j++) {
				c.add(table.get(i).get(j));
			}
			copy.add(c);
		}
		ArrayList<ArrayList<Object>> filtered = new ArrayList<ArrayList<Object>>();
		int index =-1;
		for(int i = 0; i < allColumns.size(); i++) {
			if (allColumns.get(i).equalsIgnoreCase(column)) {
				index = i;
			}
		}
		ArrayList<Object> c = (ArrayList<Object>) copy.get(index).clone();
		for (int i = 0; i < c.size(); i++) {
			if ((Integer)c.get(i) >= (Integer)reference) {
				for (int j = 0; j < copy.size(); j++) {
					copy.get(j).remove(i);
				}
				c.remove(i);
				i--;
			}
			
		}
		ColumnsFilter f = new ColumnsFilter(allColumns, columns);
		filtered = f.satisfyCondition(copy);
		return filtered;
	}

}
