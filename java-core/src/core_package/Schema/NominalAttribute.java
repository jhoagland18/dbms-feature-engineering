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
	 * 
	 * @param attributeName
	 * @param parentTable
	 * @param importantValues the values used in where conditions.
	 */
	public NominalAttribute(String attributeName, String dimension,Table parentTable, ArrayList<String> importantValues)  {
		super(attributeName,dimension, parentTable);
		this.importantValues = importantValues;
	}

}
