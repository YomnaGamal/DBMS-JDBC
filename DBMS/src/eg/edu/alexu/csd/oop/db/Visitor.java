package eg.edu.alexu.csd.oop.db;

import java.sql.SQLException;

import eg.edu.alexu.csd.oop.db.cs11.CreateDatabase;
import eg.edu.alexu.csd.oop.db.cs11.CreateTable;
import eg.edu.alexu.csd.oop.db.cs11.DeleteFromTable;
import eg.edu.alexu.csd.oop.db.cs11.DropDatabase;
import eg.edu.alexu.csd.oop.db.cs11.DropTable;
import eg.edu.alexu.csd.oop.db.cs11.InsertIntoTable;
import eg.edu.alexu.csd.oop.db.cs11.SelectFromTable;
import eg.edu.alexu.csd.oop.db.cs11.UpdateTable;

public interface Visitor {

	public void visit(CreateDatabase createdb);
	public void visit(CreateTable createTable);
	public void visit(DropDatabase dropdb);
	public void visit(DropTable dropTabale);
	public void visit(SelectFromTable select);
	public void visit(InsertIntoTable insert);
	public void visit(UpdateTable update);
	public void visit(DeleteFromTable delete);
}
