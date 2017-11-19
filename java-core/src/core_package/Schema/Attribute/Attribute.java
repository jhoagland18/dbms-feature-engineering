package core_package.Schema.Attribute;

import core_package.Main;
import core_package.Schema.Table;

public class Attribute {

	final static String ATTRIBUTE_TYPE_NUMERICAL="attribute_type_numerical";
	final static String ATTRIBUTE_TYPE_CATEGORICAL="attribute_type_categorical";
	final static String ATTRIBUTE_TYPE_DATE="attribute_type_date";
	
	private String name;
	private Table parentTable;
	private boolean isPrimaryKey;

	protected Attribute(String attributeName, boolean isPKey, Table t) {
		this.name=attributeName;
		this.parentTable=t;
		this.isPrimaryKey=isPKey;
	}
	
	public String getAttributeName() {
		return name;
	}
	
	public boolean isPKey() {
		return isPrimaryKey;
	}

	public Table getParentTable() {
		return parentTable;
	}
}
