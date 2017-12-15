package core_package.Pathfinding;
import java.util.ArrayList;

import core_package.Schema.*;

public class Path {
	
	private ArrayList<Table> tables;
	public ArrayList<Table> getTables() {
		return tables;
	};

	private ArrayList<Relationship> relationships;
	public ArrayList<Relationship> getRelationships() {
		return relationships;
	};

	
	public Path(Table first) {
		tables = new ArrayList<>();
		relationships = new ArrayList<>();
		tables.add(first);
	}
	
	public void addRelationship(Relationship r) {
		relationships.add(r);
		tables.add(r.getTable2());
	}
	
	public Table getHead() {return tables.get(tables.size() - 1);}
	
	public int getLength() {return tables.size();}
	
	public Path Copy() {
		Path toReturn = new Path(tables.get(0));
		for (int i=1;i<tables.size();i++)
			toReturn.getTables().add(tables.get(i));
		for (Relationship t : relationships)
			toReturn.getRelationships().add(t);
		return toReturn;
	}
	
	public String toString() {
		String res = "";
		for (int i =0;i<tables.size()-1;i++)
			res += tables.get(i).getName() + "->";
		return res + getHead().getName();
	}
}
