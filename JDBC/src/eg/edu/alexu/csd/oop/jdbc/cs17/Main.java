package eg.edu.alexu.csd.oop.jdbc.cs17;

import java.io.File;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.Scanner;

public class Main {

	public static void main(String[] args) {
		String url="jdbc:xmldb://localhost";
		String next="";
		
		Driver driver = new SQLDriver();
		Properties info = new Properties();
        File dbDir = new File("test");
        info.put("path", dbDir.getAbsoluteFile());
        Connection connection;
        Scanner sc=new Scanner(System.in);
        Statement statement;
       // String num="";
        try {
			connection = driver.connect(url, info);
			statement=connection.createStatement();
			System.out.println("Enter a query to execute, 1 to add queries to the batch then 2 to terminate,"
					+ " 3 to execute batch and -1 to exit.");
			while(!next.equals("-1")){
				next=sc.nextLine();
				if(!next.equals("-1")) {
					if (next.equals("1")) {
						next=sc.nextLine();
						while (!next.equals("2")) {
							statement.addBatch(next);
							next=sc.nextLine();
						}
					} else if (next.equals("3")) {
						statement.executeBatch();
					} else {
						statement.execute(next);
					}
				}
			}
			statement.close();
			connection.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
