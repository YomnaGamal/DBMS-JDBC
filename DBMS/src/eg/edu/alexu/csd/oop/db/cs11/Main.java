package eg.edu.alexu.csd.oop.db.cs11;

import java.sql.SQLException;
import java.util.Scanner;

import eg.edu.alexu.csd.oop.db.Query;

public class Main {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String query="";
		Scanner sc=new Scanner(System.in);
		InsertIntoTable insert=new InsertIntoTable();
		SelectFromTable select=new SelectFromTable();
		DeleteFromTable delete=new DeleteFromTable();
		UpdateTable update=new UpdateTable();
		CreateDatabase createDB=new CreateDatabase();
		DropDatabase dropDB=new DropDatabase();
		CreateTable createTable=new CreateTable();
		DropTable dropTable=new DropTable();
		Query[] queries=new Query[8];
		queries[0]=insert;
		queries[1]=select;
		queries[2]=delete;
		queries[3]=update;
		queries[4]=createDB;
		queries[5]=dropDB;
		queries[6]=createTable;
		queries[7]=dropTable;
        DBMS db=new DBMS();
		AccessDatabase access=new AccessDatabase(db, queries);
		System.out.println("Enter SQL queries, or -1 to exit.");
		while(!query.equals("-1")) {
			query=sc.nextLine();
			if (!query.equals("-1")) {
				try {
					access.validateSyntax(query);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		sc.close();
	}

}
