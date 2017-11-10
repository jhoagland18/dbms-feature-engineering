package core_package;

import java.util.ArrayList;

public class Attribute extends ArrayList {
	
	private String name;
	private Table parentTable;
	private boolean isPKey;
	boolean isFKey;
	

	public Attribute(String attributeName, boolean isPKey, boolean isFKey, Table t) {
		this.name=attributeName;
		parentTable=t;
		this.isPKey=isPKey;
		this.isFKey=isFKey;
		// TODO Auto-generated constructor stub
	}
	
	public String getAttributeName() {
		return name;
	}
	
	public boolean isPKey() {
		return isPKey;
	}
	
	public boolean isFKey() {
		return isFKey;
	}

}
