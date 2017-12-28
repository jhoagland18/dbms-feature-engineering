package core_package.Schema;


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


	/**
	 * Constructs an attribute object.
	 * @param attributeName the name of the attribute.
	 * @param dimension the dimension of the attribute.
	 */
	protected Attribute(String attributeName, String dimension) {
		this.name=attributeName;
		this.dimension=dimension;
	}
	
	@Override
	public String toString() {
		return getAttributeName();
	}

}