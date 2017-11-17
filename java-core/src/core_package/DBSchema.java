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
		Relationship a = new Relationship(t1, t2, a1, a2, Relationship.ONE_TO_ONE);
		relationships.add(a);
	}

	public void createPaths(ArrayList<Path> toReturn, Table targetTable, int max_length) {
		PathfinderController controller = new PathfinderController(Environment.MAX_THREADS, max_length);
		controller.createPaths(targetTable, toReturn);
	}
}
