package core_package.Schema;

import core_package.Main;



/**
 * This class represents an attribute. There are several types of attributes: nominal, numeric, id, date, timestamp
 *
 */
public class Attribute {

	protected String name;
	
	/**
	 * 
	 * @return the name of the attribute.
	 */
	public String getAttributeName() {
		return name;
	}
	
	protected String dimension;
	
	/**
	 * 
	 * @return the dimension of the attribute.
	 */
	public String getDimension() {
		return dimension;
	}

	protected Table parentTable;
	
	/**
	 * @return the table containing the attribute.
	 */
	public Table getParentTable() {
		return parentTable;
	}
	

	/**
	 * Constructs an attribute object.
	 * @param attributeName the name of the attribute.
	 * @param dimension the dimension of the attribute.
	 * @param parentTable the table containing the attribute.
	 */
	protected Attribute(String attributeName, String dimension, Table parentTable) {
		this.name=attributeName;
		this.dimension=dimension;
		this.parentTable=parentTable;
	}
	

}