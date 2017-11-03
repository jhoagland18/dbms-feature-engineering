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

	
	public void createPaths(Path partial, ArrayList<Path> toReturn, Table targetTable, int max_length) {
		if (partial== null) {
			partial = new Path();
		}
		
		System.out.println("this is partial: " + partial.toString());
		
		if (partial.getRelationships().size() == max_length) {
			System.out.println("adding to return due to length");
			toReturn.add(partial);
			return;
		}
		
		if (partial.getRelationships().size() != 0) {
			if (partial.getLastRelationship().getTables()[0].getRelationships().size() < 2 && partial.getLastRelationship().getTables()[1].getRelationships().size() < 2) {
				System.out.println("adding to return due to lack of paths");
				toReturn.add(partial);
				return;
			}
			System.out.println("Greater than zero but more possible paths");
		}
		
		System.out.println("test");

		Table nextRelTable = null;
		System.out.println("num rels:" + partial.getRelationships().size());
		if (partial.getRelationships().size() >= 2) {
			Relationship secondToLast = partial.getSecondToLastRelationship();
			Relationship last = partial.getLastRelationship();
			

				//Table nextRelTable = null;
				if (last.getTables()[0] != secondToLast.getTables()[0] && last.getTables()[0] != secondToLast.getTables()[1]) {
					nextRelTable = last.getTables()[0];
					for (Relationship rel : nextRelTable.getRelationships()) {
						if (rel != last) {
							Path p2 = new Path(partial);
							//if (targetTable.getRelationships())
							p2.addRelationship(rel);
							
							//partial.addRelationship(rel);
							createPaths(p2, toReturn, targetTable, max_length);
							
						}

					}
				}
				else if (last.getTables()[1] != secondToLast.getTables()[0] && last.getTables()[1] != secondToLast.getTables()[1]) {
					nextRelTable = last.getTables()[1];
					for (Relationship rel : nextRelTable.getRelationships()) {
						if (rel !=last) {
							Path p2 = new Path(partial);
							//if (targetTable.getRelationships())
							p2.addRelationship(rel);
							
							//partial.addRelationship(rel);
							createPaths(p2, toReturn, targetTable, max_length);
						}
					}
				}
		}
		else if (partial.getRelationships().size() == 1){
			nextRelTable = partial.getLastRelationship().getTables()[0];
			for (Relationship rel : nextRelTable.getRelationships()) {
				if (rel != partial.getLastRelationship()) {
					Path p2 = new Path(partial);
					//if (targetTable.getRelationships())
					p2.addRelationship(rel);
					
					//partial.addRelationship(rel);
					createPaths(p2, toReturn, targetTable, max_length);
				}
			}
			
			
			nextRelTable = partial.getLastRelationship().getTables()[1];
			for (Relationship rel : nextRelTable.getRelationships()) {
				if (partial.getLastRelationship() !=rel) {
					Path p2 = new Path(partial);
					//if (targetTable.getRelationships())
					p2.addRelationship(rel);
					
					//partial.addRelationship(rel);
					createPaths(p2, toReturn, targetTable, max_length);
				}
			}				
		}
		else {
			nextRelTable = targetTable;
			for (int i = 0; i < nextRelTable.getRelationships().size(); i++) {
				
				Relationship rel=nextRelTable.getRelationships().get(i);
			//for (Relationship rel : nextRelTable.getRelationships()) {
				System.out.println(rel.toString());
				Path p2 = new Path(partial);
				//if (targetTable.getRelationships())
				
				p2.addRelationship(rel);
				
				//partial.addRelationship(rel);
				createPaths(p2, toReturn, targetTable, max_length);
			}	
		}
	}
}
