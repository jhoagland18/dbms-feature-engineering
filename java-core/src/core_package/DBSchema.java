package core_package;

import java.util.ArrayList;

public class DBSchema {

	private ArrayList<Table> tables;
	private ArrayList<Relationship> relationships;
	
	public DBSchema() {
		relationships = new ArrayList<Relationship>();
		tables = new ArrayList<Table>();
	}

	public void addTable(Table p) {
		tables.add(p);
	}

	public void createRelationship(Table t1, Table t2, Attribute a1, Attribute a2) {
		Relationship a = new Relationship (t1, t2, a1, a2);
		relationships.add(a);
	}

	public void createPaths(Path partial, ArrayList<Path> toReturn, Table targetTable, int max_length) { //adds paths to list parameter up to max_length
		if (partial== null) { //initlaize partial on first run
			partial = new Path();
		}

		System.out.println("partial: " + partial.toString()); //check path at run
		
		if (partial.getRelationships().size() == max_length) { //if path is at max_length, add to toReturn and end recursive loop
			toReturn.add(partial);
			return;
		}
		
		if (partial.getRelationships().size() != 0) {
			if (partial.getLastRelationship().getTables()[0].getRelationships().size() < 2
					&& partial.getLastRelationship().getTables()[1].getRelationships().size() < 2) {
				//if the two tables in the last added relationship only link to each other, return
				toReturn.add(partial);
				return;
			}
		}

		Table nextRelTable = null;

		System.out.println("num relationships:" + partial.getRelationships().size());

		if (partial.getRelationships().size() > 1) { //if more 1 relationship in path
			Relationship secondToLast = partial.getSecondToLastRelationship();
			Relationship last = partial.getLastRelationship();

			/*
			The block of code below identifies which table to build off of for the next relationship in partial.
			 */

				if (last.getTables()[0] != secondToLast.getTables()[0] && last.getTables()[0] != secondToLast.getTables()[1]) { //check if table 1 was used in the previous relationship link
					nextRelTable = last.getTables()[0];
					createNewPaths(nextRelTable, partial, toReturn, targetTable, max_length);
				}
				else if (last.getTables()[1] != secondToLast.getTables()[0] && last.getTables()[1] != secondToLast.getTables()[1]) { //check if table 2 was used in the previous relationship link
					nextRelTable = last.getTables()[1];
					createNewPaths(nextRelTable, partial, toReturn, targetTable, max_length);
				}
			}
		}
		else if (partial.getRelationships().size() == 1){ //if only one relationship in partial, build paths off of each table.
			nextRelTable = partial.getLastRelationship().getTables()[0];
			createNewPaths(nextRelTable, partial, toReturn, targetTable, max_length);

			nextRelTable = partial.getLastRelationship().getTables()[1];
			createNewPaths(nextRelTable, partial, toReturn, targetTable, max_length);
		} else { //if no paths, start all paths off of target table
			nextRelTable = targetTable;
			createNewPaths(nextRelTable, partial, toReturn, targetTable, max_length);
		}
	}

	public void createNewPaths(Table nextRelTable, Path partial, ArrayList<Path> toReturn, Table targetTable, int max_length) {
		//iterates through relationships of next table in relationship and recursively calls createPaths to generate all partials

		for (Relationship rel : nextRelTable.getRelationships()) {
			if(partial.getLength()>0) { //if partial has more than one element
				if (rel != partial.getLastRelationship()) { //add all relationships that are not the previous one used in partial
					Path p2 = new Path(partial);
					p2.addRelationship(rel);
					createPaths(p2, toReturn, targetTable, max_length);
				}
			} else { //if partial only has one element, add all relationships possible
				Path p2 = new Path(partial);
				p2.addRelationship(rel);
				createPaths(p2, toReturn, targetTable, max_length);
			}
		}
	}
}
