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
	}
	
	public int addAttribute(String attributeName, boolean isKey) {
		if(isKey && isPrimaryKeySet) {
			return -1;
		} else {
			if(isKey) {
				isPrimaryKeySet=true;
			}
			Attribute a = new Attribute(attributeName, isKey);
			attributes.add(a);
			return 1;
		}
	}
	
	public void addRelationship(Table targetTable, String attName) {
		Relationship r = new Relationship(targetTable, attName);
		relationships.add(r);
	}
	
	public String getTableName() {
		return name;
	}
	
	public boolean isPKSet() {
		return isPrimaryKeySet;
	}

}
