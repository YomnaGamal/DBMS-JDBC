package eg.edu.alexu.csd.oop.db.cs11;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Calendar;

public class Cache implements Runnable {
	// for insert and update
	private ArrayList<String> tables;
	private ArrayList<HashMap<String, String>> changes;
	private boolean read;
	private java.util.Date dateofExpiration = null;

	/*
	 * Runnable runnable = new Runnable() {
	 * 
	 * @Override public void run() { // TODO Auto-generated method stub while (true)
	 * { isExpired(); } }
	 * 
	 * }; Thread thread = new Thread(runnable);
	 */
	public Cache() {
		tables = new ArrayList<String>();
		changes = new ArrayList<HashMap<String, String>>();
		read = false;
		dateofExpiration = new java.util.Date();
		Calendar cal = Calendar.getInstance();
		cal.setTime(dateofExpiration);
		cal.add(cal.MINUTE, 1);
		dateofExpiration = cal.getTime();
		// thread.start();
	}

	public void addcommand(String table, HashMap<String, String> change) {
		tables.add(table);
		changes.add(change);
	}

	public void checkReadOrClose(boolean read) {
		this.read = read;
	}

	private void isExpired() {
		if (dateofExpiration.before(new java.util.Date()) || read) {
			save();
			new Cache();
		}
	}

	private void save() {
		Files x = new Files();
		for (int i = 0; i < tables.size(); i++) {
			String table = tables.get(i);
			HashMap<String, String> change = changes.get(i);
			x.insertIntoTable(table, change);
		}
	}

	@Override
	public void run() {
		while (true) {
			isExpired();
			/*
			 * System.out.println(isExpired()); try { sleep(1000); } catch
			 * (InterruptedException e) { // TODO Auto-generated catch block
			 * e.printStackTrace(); }
			 */
		}
	}
	// execute after minute or when read
}
