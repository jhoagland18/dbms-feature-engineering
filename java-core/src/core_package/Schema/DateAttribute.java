package core_package.Schema;


/**
 * An attribute that only has date values that are unrelated to the moment when a row was available.
 *
 */
public class DateAttribute extends Attribute {
	
	public DateAttribute(String attributeName,String dimension) throws Exception  {
		super(attributeName, dimension);
		throw new Exception("DateAttributes are not supported. You need to transform a date to a number first (e.g., birth date -> age");
	}

}
