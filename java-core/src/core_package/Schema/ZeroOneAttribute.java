package core_package.Schema;


/**
 * An attribute that only has two numeric values 0 and 1
 *
 */
public class ZeroOneAttribute extends Attribute {
	
	public ZeroOneAttribute(String attributeName, Table parentTable)  {
		super(attributeName,"zero-one", parentTable);
	}

}
