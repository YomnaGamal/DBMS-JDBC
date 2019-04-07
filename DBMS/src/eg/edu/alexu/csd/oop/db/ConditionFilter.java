package eg.edu.alexu.csd.oop.db;

import java.util.ArrayList;

public interface ConditionFilter {

	public ArrayList<ArrayList<Object>> satisfyCondition(ArrayList<ArrayList<Object>> table);
}
