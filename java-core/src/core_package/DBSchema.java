package core_package;

import java.util.ArrayList;

public class DBSchema {

	ArrayList<Table> tables;
	ArrayList<Relationship> relationships;
	
	public DBSchema() {
		
	}

	public void addTable(Table p) {
		// TODO Auto-generated method stub
		
	}

	public void addRelationship(Table p, Table cl, String string, String string2) {
		// TODO Auto-generated method stub
	
	}
	
	
	public Path finishPath(ArrayList<Table> current, int max_length) {
		for (int i=tables.size(); i>0; i++) {
			ArrayList<Table> current2 = new ArrayList<Table>();
			current2.equals(current);
			
		}
		return finishPath(current, max_length);
	}
}
