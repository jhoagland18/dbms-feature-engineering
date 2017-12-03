package core_package.Pathfinding;

import core_package.Schema.Relationship;
import core_package.Schema.Table;

import java.util.ArrayList;

public class Path {

	private ArrayList<PathLink> relationships;
	
	public Path () {
		relationships = new ArrayList<PathLink>();
	}
	public Path(Path p) {
		relationships = new ArrayList<PathLink>(p.getLinks());
	}
	
	public Path addRelationship(Relationship rel, Table last) {
		PathLink link = new PathLink(rel, last);
		relationships.add(link);
		return this;
	}
	
	public Relationship getLastRelationship() {
		return relationships.get(relationships.size()-1).getRelationship();
	}

	public Relationship getSecondToLastRelationship() {
		return relationships.get(relationships.size()-2).getRelationship();
	}

	public PathLink getLastLink() {
		return relationships.get(relationships.size()-1);
	}

	public Relationship getRelationship(int i) {
		return relationships.get(i).getRelationship();
	}
	
	public ArrayList<Relationship> getRelationships() {
		ArrayList<Relationship> rels = new ArrayList<>();
		for(PathLink link : relationships) {
			rels.add(link.getRelationship());
		}
		return rels;
	}

	public ArrayList<PathLink> getLinks() {
		return relationships;
	}

	public int getLength() {
		return relationships.size();
	}

	public String toString() {

		String s = "path: ";
		for (PathLink rels : relationships) {
			s += "[ "+rels.toString()+" ] ";
		}
		return s;
	}
}
