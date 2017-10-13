package core_package;

import java.util.ArrayList;

public class Table {
	private String name;
	private boolean isPrimaryKeySet=false;
	private ArrayList<Attribute> attributes;
	private ArrayList<Relationship> relationships;
	
	public Table(String name) {
		this.name=name;
		attributes = new ArrayList<Attribute>();
		relationships = new ArrayList<Relationship>();
	}
	
	public int addAttribute(String attributeName, boolean isKey) {
		if(isKey && isPrimaryKeySet) {
			return -1;
		} else {
			if(isKey) {
				isPrimaryKeySet=true;
			}
			Attribute a = new Attribute(attributeName, isKey, this);
			attributes.add(a);
			return 1;
		}
	}
	
	public void addRelationship (Relationship rel) {
		//Relationship rel = new Relationship(targetTable, attribute);
		relationships.add(rel);
	}
	
	public String getTableName() {
		return name;
	}
	
	public boolean isPKSet() {
		return isPrimaryKeySet;
	}
	
	public ArrayList<Relationship> getRelationships () {
		return relationships;
	}


}
