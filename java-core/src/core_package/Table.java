package core_package;

import java.util.ArrayList;

public class Table {
	private String name;
	private boolean isPrimaryKeySet=false;
	//private boolean isForeignKeySet=false;
	private ArrayList<Attribute> attributes;
	private ArrayList<Relationship> relationships;
	private ArrayList<ForeignKey> foreignKeys;
	
	
	public Table(String name) {
		this.name=name;
		attributes = new ArrayList<Attribute>();
		relationships = new ArrayList<Relationship>();
		foreignKeys = new ArrayList<ForeignKey>();
	}
	
	public int addAttribute(String attributeName, boolean isPKey) {
		if(isPKey && isPrimaryKeySet) {
			return -1;
		} else {
			if(isPKey) {
				isPrimaryKeySet=true;
			}	
			
			Attribute a = new Attribute(attributeName, isPKey, this);
	
			attributes.add(a);
			return 1;
		}
		
	}
	
	public Attribute addForeignKey(String attributeName, Table link) {
		ForeignKey fK = new ForeignKey(attributeName, false, this, link);
		foreignKeys.add(fK);
		return fK;
	}
	
	public void addRelationship (Relationship rel) {
		//Relationship rel = new Relationship(targetTable, attribute);
		relationships.add(rel);
		Main.printVerbose("Adding relationship for table: " + name);
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
	
	public String toString() {
		return name;
	}
	public Attribute getAttribute(String name) {
		for (int i = 0; i<attributes.size(); i++) {
			if (attributes.get(i).equals(name)) {
				return attributes.get(i);
			}
		}
		return null;
	}
	public ArrayList<Attribute> getAttributes() {
		return attributes;
	}
	
	public Attribute getPKey() {
		for (int i =0; i < attributes.size(); i++ ) {
			if (attributes.get(i).isPKey())
				return attributes.get(i);
		}
		return null;
	}
	
	public ArrayList<ForeignKey> getFKeys() {
		return foreignKeys;
	}
}
