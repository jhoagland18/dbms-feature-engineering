package core_package;

import java.util.ArrayList;

public class DBSchema {

	private ArrayList<Table> tables;
	private ArrayList<Relationship> relationships;
	//private ArrayList<Path> paths;
	
	public DBSchema() {
		//paths = new ArrayList<Path>();
		relationships = new ArrayList<Relationship>();
		tables = new ArrayList<Table>();
	}

	public void addTable(Table p) {
		tables.add(p);
		// TODO Auto-generated method stub
	}

	public void createRelationship(Table t1, Table t2, Attribute a1, Attribute a2) {
		Relationship a = new Relationship (t1, t2, a1, a2);
		relationships.add(a);
	}
	
	public ArrayList<Path> buildPaths(ArrayList<Path> paths, Table baseTable, int max_length) {
		if (paths == null) {
			paths=new ArrayList<Path>();
			 ArrayList<Relationship> basepaths= baseTable.getRelationships();
			 System.out.println(basepaths.size());
			 for (int j=0; j<basepaths.size(); j++) {
				 //Path a = new Path();
				 System.out.println("are you running");
				 paths.add(new Path().addRelationship(basepaths.get(j)));
			 }
		}
		if {
			boolean isComplete=true;
			for (Path p : paths) {
				if (!(p.getRelationships().size() >=3)) {
					Relationship lastRel = p.getLastRelationship();
					Table[] lastRelationshipTables= lastRel.getTables();
					Relationship secondToLastRel;
					if (p.getRelationships().size() > 1) {
						secondToLastRel = p.getSecondToLastRelationship();
						Table[] secondToLastRelationshipTables=secondToLastRel.getTables();
						Table nextRelTable=null;
						
						if (lastRelationshipTables[0] == secondToLastRelationshipTables[0]) {
							nextRelTable=lastRelationshipTables[1];
						}
						else if (lastRelationshipTables[1] == secondToLastRelationshipTables[0]){
							nextRelTable=lastRelationshipTables[0];
						}
						else if (lastRelationshipTables[0] == secondToLastRelationshipTables[1]){
							nextRelTable=lastRelationshipTables[1];
						}
						else {
							nextRelTable=lastRelationshipTables[0];
						}
						if (nextRelTable.getRelationships().size()>0) {
							isComplete=false;
							
						}
						System.out.println(nextRelTable.toString());
					}
					else {
						Table[] relTables = p.getRelationship(0).getTables();
						if (tables.contains(relTables[0]) && tables.contains(relTables[1]))
							isComplete=true;
						else
							isComplete=false;
						
					}
					
				}
			}
		}
		//ArrayList<Table> current2 = new ArrayList<Table>();
		
		for (int i=0; i<paths.size(); i++) {
			
			Relationship lastRel = paths.get(i).getLastRelationship();
			Table[] lastRelationshipTables= lastRel.getTables();
			//System.out.println(lastRelationshipTables[0] + ", " +lastRelationshipTables[1]);
			
			if (lastRel.hasTable(lastRelationshipTables[0]) ^ lastRel.hasTable(lastRelationshipTables[1])){
				System.out.println("hello");
				Table t = lastRel.hasTable(lastRelationshipTables[0]) ? lastRelationshipTables[0] : lastRelationshipTables[1];
				ArrayList<Relationship> tableRelationships = t.getRelationships();
				for (int j=0; j<tableRelationships.size(); j++) {
					paths.get(i).addRelationship(tableRelationships.get(j));
				}
			}
					//getRelationships();
			return buildPaths(paths, baseTable, max_length);

		}
		return paths;

	}
	
	/*public void printPaths() {
		for (int i = 0; i <paths.size()-1; i++) {
			paths.get(i).getRelationships().toString();
		}
	}*/
	
}
