package eg.edu.alexu.csd.oop.db.cs11;

import java.util.ArrayList;

import eg.edu.alexu.csd.oop.db.ConditionFilter;

public class Not implements ConditionFilter {

	private ConditionFilter condition;

	public Not(ConditionFilter condition) {
		this.condition = condition;
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
		ArrayList<ArrayList<Object>> rows = transpose(copy);
		ArrayList<ArrayList<Object>> inverse = condition.satisfyCondition(copy);	
		ArrayList<ArrayList<Object>> inverseRows = transpose(inverse);
		for (int i = 0; i < inverseRows.size(); i++) {
			for (int j = 0; j < rows.size(); j++) {
				if (inverseRows.get(i).equals(rows.get(j))) {
					rows.remove(j);
					inverseRows.remove(i);
					i--;
					j--;
					break;
				}
			}
		}
		return transpose(rows);
	}

	private ArrayList<ArrayList<Object>> transpose(ArrayList<ArrayList<Object>> columns) {
		ArrayList<ArrayList<Object>> rows = new ArrayList<ArrayList<Object>>();
		for (int j = 0; j < columns.get(0).size(); j++) {
			ArrayList<Object> r = new ArrayList<Object>();
			for (int i = 0; i < columns.size(); i++) {
				r.add(columns.get(i).get(j));
			}
			rows.add(r);
		}
		return rows;
	}
}
