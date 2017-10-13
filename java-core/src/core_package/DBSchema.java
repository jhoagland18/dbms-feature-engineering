package core_package;

import java.util.ArrayList;

public class DBSchema {

	private ArrayList<Table> tables;
	private ArrayList<Relationship> relationships;
	private ArrayList<Path> paths;
	
	public DBSchema() {
		paths = new ArrayList<Path>();
		relationships = new ArrayList<Relationship>();
		tables = new ArrayList<Table>();
	}

	public void addTable(Table p) {
		tables.add(p);
		// TODO Auto-generated method stub
		
	}

	public void createRelationship(Table t1, Table t2, Attribute a1, Attribute a2) {
		Relationship a = new Relationship (t2, t2, a1, a2);
	}
	public ArrayList<Path> buildPaths(ArrayList<Relationship> current, Table baseTable, int max_length) {
		System.out.println("WORK");
		if (current == null) {
			current = baseTable.getRelationships();
		}
		//ArrayList<Table> current2 = new ArrayList<Table>();
		for (int i=0; i<paths.size()-1; i++) {
			
			Relationship lastRel = paths.get(i).getLastRelationship();
			Table[] lastRelationshipTables= lastRel.getTables();
			if (lastRel.hasTable(lastRelationshipTables[0]) ^ lastRel.hasTable(lastRelationshipTables[1])){
				
				Table t = lastRel.hasTable(lastRelationshipTables[0]) ? lastRelationshipTables[0] : lastRelationshipTables[1];
				ArrayList<Relationship> tableRelationships = t.getRelationships();
				for (int j=0; j<tableRelationships.size()-1; j++) {
					paths.get(i).addRelationship(tableRelationships.get(j));
					
				}
			}
					//getRelationships();
			return buildPaths(current, baseTable, max_length);

		}
		return paths;

	}
	
	public void printPaths() {
		for (int i = 0; i <paths.size()-1; i++) {
			paths.get(i).getRelationships().toString();
		}
	}
	
}
