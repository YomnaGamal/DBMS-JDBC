package eg.edu.alexu.csd.oop.db.cs11;

import java.util.ArrayList;

import eg.edu.alexu.csd.oop.db.ConditionFilter;

public class And implements ConditionFilter {

	private ConditionFilter condition1;
	private ConditionFilter condition2;

	public And(ConditionFilter condition1, ConditionFilter condition2) {
		this.condition1 = condition1;
		this.condition2 = condition2;
	}

	@Override
	public ArrayList<ArrayList<Object>> satisfyCondition(ArrayList<ArrayList<Object>> table) {
		// TODO Auto-generated method stub
		ArrayList<ArrayList<Object>> c1 = condition1.satisfyCondition(table);
		return condition2.satisfyCondition(c1);
	}

}
