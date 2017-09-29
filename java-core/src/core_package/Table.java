package core_package;

import java.util.ArrayList;

public class Table {
	private String name;
	private boolean isPrimaryKeySet=false;
	private ArrayList<Attribute> attributes;
	
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
	
	public String getTableName() {
		return name;
	}
	
	public boolean isPKSet() {
		return isPrimaryKeySet;
	}

}
