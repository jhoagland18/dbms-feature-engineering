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

	public void addTables(ArrayList<Table> newTables) {
		tables.addAll(newTables);
	}

	public ArrayList<Table> getTables() {
		return tables;
	}

	public void createRelationship(Attribute a1, Attribute a2, int cardinality) {
		Relationship a = new Relationship(a1, a2, cardinality);
		relationships.add(a);
	}

	public void createPaths(ArrayList<Path> toReturn, String targetTableName, int max_length) {

		PathfinderController controller = new PathfinderController(Environment.MAX_THREADS, max_length);
		controller.createPaths(getTableFromName(targetTableName), toReturn);
	}

    private Table getTableFromName(String name) {
        for(int i=0; i<tables.size(); i++) {
            if(tables.get(i).getTableName().equalsIgnoreCase(name)) {
                return tables.get(i);
            }
        }
        return null;
    }


	public String toString() {
		String toReturn="Tables:\n";
		for(Table t : tables) {
			toReturn+=t.toString()+"\n";
		}

		toReturn+="\nRelationships:\n";

		for(Relationship r: relationships) {
			toReturn+=r.toString()+"\n";
		}

		return toReturn;
	}
}
