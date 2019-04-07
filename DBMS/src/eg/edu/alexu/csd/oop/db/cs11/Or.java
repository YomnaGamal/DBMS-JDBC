package eg.edu.alexu.csd.oop.db.cs11;

import java.util.ArrayList;

import eg.edu.alexu.csd.oop.db.ConditionFilter;

public class Or implements ConditionFilter {

	private ConditionFilter condition1;
	private ConditionFilter condition2;

	public Or(ConditionFilter condition1, ConditionFilter condition2) {
		this.condition1 = condition1;
		this.condition2 = condition2;
	}

	@SuppressWarnings("unchecked")
	@Override
	public ArrayList<ArrayList<Object>> satisfyCondition(ArrayList<ArrayList<Object>> table) {
		// TODO Auto-generated method stub
		ArrayList<ArrayList<Object>> copy1 = new ArrayList<ArrayList<Object>>();
		ArrayList<ArrayList<Object>> copy2 = new ArrayList<ArrayList<Object>>();
		//copy1 = (ArrayList<ArrayList<Object>>) table.clone();
		//copy2 = (ArrayList<ArrayList<Object>>) table.clone();
		for (int i = 0; i < table.size(); i++) {
			ArrayList<Object> c = new ArrayList<Object>();
			for(int j = 0; j < table.get(i).size(); j++) {
				c.add(table.get(i).get(j));
			}
			copy1.add(c);
			copy2.add(c);
		}
		ArrayList<ArrayList<Object>> c1 = new ArrayList<ArrayList<Object>>();
		ArrayList<ArrayList<Object>> c2 = new ArrayList<ArrayList<Object>>();
		c1 = (ArrayList<ArrayList<Object>>) condition1.satisfyCondition(copy1).clone();
		c2 = (ArrayList<ArrayList<Object>>) condition2.satisfyCondition(copy2).clone();
		ArrayList<ArrayList<Object>> r1 = (ArrayList<ArrayList<Object>>) transpose(c1).clone();
		ArrayList<ArrayList<Object>> r2 = (ArrayList<ArrayList<Object>>) transpose(c2).clone();
		ArrayList<ArrayList<Object>> x = new ArrayList<ArrayList<Object>>();
		for (int j = 0; j < r2.size(); j++) {
			if (!r1.contains(r2.get(j))) {
				//r1.add(r2.get(j));
				x.add(r2.get(j));
			}
		}
		for (int i = 0; i < x.size(); i++) {
			r1.add(x.get(i));
		}
		return transpose(r1);
	}

	private ArrayList<ArrayList<Object>> transpose(ArrayList<ArrayList<Object>> columns) {
		ArrayList<ArrayList<Object>> rows = new ArrayList<ArrayList<Object>>();
		int c = columns.get(0).size();
		for (int j = 0; j < c; j++) {
			ArrayList<Object> r = new ArrayList<Object>();
			for (int i = 0; i < columns.size(); i++) {
				r.add(columns.get(i).get(j));
			}
			rows.add(r);
		}
		return rows;
	}
}
