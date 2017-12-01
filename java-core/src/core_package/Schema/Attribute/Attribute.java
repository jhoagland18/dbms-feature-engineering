package core_package.Schema.Attribute;

import core_package.Main;
import core_package.Schema.Relationship;
import core_package.Schema.Table;

public abstract class Attribute {

	final static String ATTRIBUTE_TYPE_NUMERICAL="attribute_type_numerical";
	final static String ATTRIBUTE_TYPE_CATEGORICAL="attribute_type_categorical";
	final static String ATTRIBUTE_TYPE_DATE="attribute_type_date";
	public final static String[] SQL_NUMERICAL_FUNCTIONS = {"sum", "min", "max", "count", "avg"};
	
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
	
	public abstract String fillTemplate(String[] accessPoints, Relationship rel);
}
