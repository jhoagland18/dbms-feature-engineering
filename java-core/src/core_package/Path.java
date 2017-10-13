package core_package;

import java.util.ArrayList;

public class Path {

	private ArrayList<Relationship> relationships;
	
	public Path () {
		relationships = new ArrayList<Relationship>();
	}
	
	public void addRelationship(Relationship rel) {
		relationships.add(rel);
	}
	
	public Relationship getLastRelationship() {
		return relationships.get(relationships.size()-1);
	}
	public Relationship getRelationship(int i) {
		return relationships.get(i);
	}
	
	public ArrayList<Relationship> getRelationships() {
		return relationships;
	}
}
