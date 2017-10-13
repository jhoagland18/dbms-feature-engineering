package core_package;

import java.util.ArrayList;

public class Attribute extends ArrayList {
	
	private String name;
	private Table parentTable;

	public Attribute(String attributeName, boolean isKey, Table t) {
		this.name=attributeName;
		parentTable=t;
		// TODO Auto-generated constructor stub
	}
	
	public String getAttributeName() {
		return name;
	}

}
