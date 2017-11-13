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
		Relationship a = new Relationship(t1, t2, a1, a2);
		relationships.add(a);
	}

	public void createPaths(ArrayList<Path> toReturn, Table targetTable, int max_length) {
		for (Relationship rel : targetTable.getRelationships()) {
			Path p2 = new Path();
			p2.addRelationship(rel);
			try {
				PathFinder finder = new PathFinder(p2, toReturn, targetTable, max_length);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			Main.printVerbose("Spawning new pathfinding thread");
		}
	}


	class PathFinder implements Runnable {

		private final ArrayList<Path> toReturn;
		private int max_length = 0;
		private Table targetTable = null;

		private Thread pf;
		private Path startPartial;

		public PathFinder(Path startPartial, ArrayList<Path> toReturn, Table targetTable, int max_length) throws InterruptedException {
			synchronized (toReturn) {
				this.toReturn = toReturn;
			}

			this.targetTable = targetTable;
			this.max_length = max_length;
			this.startPartial = startPartial;

			pf = new Thread(this, "pathfinder");
			pf.start();
			pf.join();
		}

		@Override
		public void run() {
			buildPartials(startPartial);
		}

		public void buildPartials(Path partial) { //adds paths to list parameter up to max_length
			if (partial == null) { //initlaize partial on first run
				partial = new Path();
			}

			Main.printVerbose("partial: " + partial.toString()); //check path at run

			if (partial.getRelationships().size() == max_length-1) { //if path is at max_length, add to toReturn and end recursive loop
				synchronized (toReturn) {
					toReturn.add(partial);
				}
				return;
			}

			if (partial.getRelationships().size() != 0) {
				if (partial.getLastRelationship().getTables()[0].getRelationships().size() < 2
						&& partial.getLastRelationship().getTables()[1].getRelationships().size() < 2) {
					//if the two tables in the last added relationship only link to each other, return
					synchronized (toReturn) {
						toReturn.add(partial);
					}
					return;
				}
			}

			Table nextRelTable = null;

			Main.printVerbose("Number of relationships in partial: " + partial.getRelationships().size());

			if (partial.getRelationships().size() > 1) { //if more 1 relationship in path
				Relationship secondToLast = partial.getSecondToLastRelationship();
				Relationship last = partial.getLastRelationship();

		/*
		The block of code below identifies which table to build off of for the next relationship in partial.
		 */

				if (last.getTables()[0] != secondToLast.getTables()[0] && last.getTables()[0] != secondToLast.getTables()[1]) { //check if table 1 was used in the previous relationship link
					nextRelTable = last.getTables()[0];
					createNewPaths(nextRelTable, partial);
				} else if (last.getTables()[1] != secondToLast.getTables()[0] && last.getTables()[1] != secondToLast.getTables()[1]) { //check if table 2 was used in the previous relationship link
					nextRelTable = last.getTables()[1];
					createNewPaths(nextRelTable, partial);
				}
			} else if (partial.getRelationships().size() == 1) { //if only one relationship in partial, build paths off of each table.
				nextRelTable = partial.getLastRelationship().getTables()[0];
				createNewPaths(nextRelTable, partial);

				nextRelTable = partial.getLastRelationship().getTables()[1];
				createNewPaths(nextRelTable, partial);
			} else { //if no paths, start all paths off of target table
				nextRelTable = targetTable;
				createNewPaths(nextRelTable, partial);
			}
		}

		public void createNewPaths(Table nextRelTable, Path partial) {
			//iterates through relationships of next table in relationship and recursively calls createPaths to generate all partials

			for (Relationship rel : nextRelTable.getRelationships()) {
				if (partial.getLength() > 0) { //if partial has more than one element
					if (rel != partial.getLastRelationship()) { //add all relationships that are not the previous one used in partial
						Path p2 = new Path(partial);
						p2.addRelationship(rel);
						buildPartials(p2);
					}
				} else { //if partial only has one element, add all relationships possible
					Path p2 = new Path(partial);
					p2.addRelationship(rel);
					buildPartials(p2);
				}
			}
		}
	}


}
