package core_package.Schema;

import java.util.ArrayList;

public class NominalAttribute extends Attribute {
	
	ArrayList<String> importantValues;
	
	/**
	 * 
	 * @return the important values of this nominal attribute.
	 */
	public ArrayList<String> getImportantValues() {
		return importantValues;
	}

	/**
	 * @param importantValues
	 */
	public void setImportantValues(ArrayList<String> importantValues) {
		this.importantValues = importantValues;
	}
	
	/**
	 * 
	 * @param attributeName
	 * @param parentTable
	 * @param importantValues the values used in where conditions.
	 */
	public NominalAttribute(String attributeName, String dimension, ArrayList<String> importantValues)  {
		super(attributeName,dimension);
		this.importantValues = importantValues;
	}

}
