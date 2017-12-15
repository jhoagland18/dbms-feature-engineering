package core_package.Schema;


/**
 * An attribute that is (or is part of) a primary key or a foreign key. No where conditions or aggregations will be generated on this attribute.
 *
 */
public class IDAttribute extends Attribute {
	
	public IDAttribute(String attributeName)  {
		super(attributeName,"");
	}

}
