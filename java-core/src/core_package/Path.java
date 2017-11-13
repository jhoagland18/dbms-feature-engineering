package core_package;

import java.util.ArrayList;

public class Path {

	private ArrayList<Relationship> relationships;
	
	public Path () {
		relationships = new ArrayList<Relationship>();
	}
	public Path(Path p) {
		relationships = p.getRelationships();
	}
	
	public Path addRelationship(Relationship rel) {
		relationships.add(rel);
		return this;
	}
	
	public Relationship getLastRelationship() {
		return relationships.get(relationships.size()-1);
	}

	public Relationship getSecondToLastRelationship() {
		return relationships.get(relationships.size()-2);
	}

	public Relationship getRelationship(int i) {
		return relationships.get(i);
	}
	
	public ArrayList<Relationship> getRelationships() {
		return relationships;
	}

	public int getLength() {
		return relationships.size();
	}

	public String toString() {

		String s = "path: ";
		for (Relationship rels : relationships) {
			s += "[ "+rels.toString()+" ] ";
		}
		return s;
	}
}
