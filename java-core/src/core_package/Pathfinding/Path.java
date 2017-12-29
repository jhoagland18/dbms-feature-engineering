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

	public void removeLastRelationship() {
		relationships.remove(relationships.size()-1);
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

		String s = "";
		for (PathLink rels : relationships) {
			s += "[ "+rels.toString()+" ] ";
		}
		s+="\n";


		for (int i = 0; i < relationships.size(); i++) {
			PathLink link = relationships.get(i);
			if(i==0) {
				if(link.getRelationship().getTables()[0]!=link.getLastTable()) {
					s+=link.getRelationship().getTables()[0].getTableName()+ " -> ";
				} else {
					s+=link.getRelationship().getTables()[1].getTableName()+ " -> ";
				}
			}

			s+=link.getLastTable().getTableName();

			if(i < relationships.size()-1)
				s+= " -> ";
		}

		return s+="\n";
	}
}
