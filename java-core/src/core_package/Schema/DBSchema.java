package core_package.Schema;

import core_package.Schema.Attribute.Attribute;
import core_package.Environment;
import core_package.Pathfinding.Path;
import core_package.Pathfinding.PathfinderController;

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

	public void createRelationship(Attribute a1, Attribute a2, int cardinality) {
		Relationship a = new Relationship(a1, a2, cardinality);
		relationships.add(a);
	}

	public void createPaths(ArrayList<Path> toReturn, Table targetTable, int max_length) {
		PathfinderController controller = new PathfinderController(Environment.MAX_THREADS, max_length);
		controller.createPaths(targetTable, toReturn);
	}
}
