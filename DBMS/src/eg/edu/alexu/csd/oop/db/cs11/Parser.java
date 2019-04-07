package eg.edu.alexu.csd.oop.db.cs11;

import eg.edu.alexu.csd.oop.db.Query;

public class Parser {

	private Query[] q;

	public Parser(Query[] q) {
		this.q = q;
	}

	public int checkSyntax(String query) {
		// TODO Auto-generated method stub
		for (int i = 0; i < q.length; i++) {
			if (q[i].interpreter(query)) {
				return i;
			}
		}
		return -1;
	}
}
