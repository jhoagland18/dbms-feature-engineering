package core_package.Schema;

import core_package.Main;
import core_package.Schema.Attribute.Attribute;

public class Relationship {

	private Table table1;
	private Table table2;
	private Attribute attribute1;
	private Attribute attribute2;

	//store relationship types
	public final static int ONE_TO_MANY=1;
	public final static int ONE_TO_ONE=0;
	
	public Relationship (Attribute att1, Attribute att2, int cardinality) {
		attribute1=att1;
		attribute2=att2;

		Main.printVerbose("att is " + att1.toString());
		Main.printVerbose("table is "+att1.getParentTable());
		table1=att1.getParentTable();
		table2=att2.getParentTable();

		addRelationshipsToTables();
	}
	
	public Table[] getTables() {
		Table[] tables = new Table[2];
		tables[0] = table1;
		tables[1] = table2;
		return tables;
	}

	public void addRelationshipsToTables() {
		table1.addRelationship(this);
		table2.addRelationship(this);
	}

	public boolean hasTable(Table t) {
		return (table1==t || table2==t) ? true : false;
	}
	
	public String toString() {
		return table1.toString() + "     " +table2.toString();
	}
}
