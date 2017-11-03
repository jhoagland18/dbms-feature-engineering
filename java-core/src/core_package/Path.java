package core_package;

import java.util.ArrayList;

public class Path {

	private ArrayList<Relationship> relationships;
	private int length;
	
	public Path () {
		relationships = new ArrayList<Relationship>();
		length=0;
	}
	public Path(Path p) {
		relationships = p.getRelationships();
		length=0;
	}
	
	public Path addRelationship(Relationship rel) {
		if (rel == null) {
			System.out.println("WHY");
		}
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
		return length;
	}
	public void setLength(int l) {
		length=l;
	}
	
	public String toString() {
		String s = "path: ";
		for (Relationship rels : relationships) {
			s = s + rels.toString();
		}
		return s;
	}
}
