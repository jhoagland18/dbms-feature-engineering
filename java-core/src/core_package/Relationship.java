package core_package;

public class Relationship {
	
	private Table table1;
	private Table table2;
	private Attribute attribute1;
	private Attribute attribute2;
	//store Relationship Types
	
	public Relationship (Table a, Table b, Attribute att1, Attribute att2) {
		table1=a;
		table2=b;

		attribute1=att1;	
		attribute2=att2;
		
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
		//System.out.println(table1.toString());
		//System.out.println(table2.toString());
	}
	public boolean hasTable(Table t) {
		return (table1==t || table2==t) ? true : false;
	}
	
	public String toString() {
		return table1.toString() + "     " +table2.toString();
	}
}
