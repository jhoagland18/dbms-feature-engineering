package core_package.Schema;

import org.omg.CORBA.NameValuePair;
import org.w3c.dom.Attr;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class Table {
	private String name;
	private ArrayList<Attribute> attributes;
	private ArrayList<Relationship> relationships;
	private ArrayList<Attribute> primaryKey;
	private HashMap<String,Attribute> attributeByName;
	private HashMap<String,ArrayList<Attribute> > attributesByDimension;
	private HashMap<Class<? extends Attribute>,ArrayList<Attribute> > attributesByType;
	private HashMap<String,HashMap<String,Integer>> rowSample;

	// GETTER METHODS
	
	/**
	 * 
	 * @return the name of the table.
	 */
	public String getName() {return name;}
	
	/**
	 * 
	 * @return all attributes.
	 */
	public ArrayList<Attribute> getAttributes() {return attributes;}
	
	/**
	 * 
	 * @return all outgoing relationships.
	 */
	public  ArrayList<Relationship> getRelationships() {return relationships;}
	
	/**
	 * 
	 * @return the primary key
	 */
	public  ArrayList<Attribute> getPrimaryKey() {return primaryKey;}
	
	/**
	 * 
	 * @param name the name of the attribute
	 * @return the attribute with that name
	 */
	public  Attribute getAttributeByName(String name) {return attributeByName.get(name);}

	/**
	 * @param name
	 * @return the attribute with that name
	 */
	public Attribute getAttrbuteByNameIgnoreCase(String name) {
		ArrayList<Attribute> allAtts = new ArrayList<>();
		allAtts.addAll(attributes);
		allAtts.addAll(primaryKey);
		for(Attribute att: allAtts) {
			if(att.getAttributeName().equalsIgnoreCase(name)) {
				return att;
			}
		}
		return null;
	}
	
	/**
	 * 
	 * @param dimension
	 * @return all attributes with the given dimension
	 */
	public  ArrayList<Attribute> getAttributesByDimension(String dimension) {
		if (!attributesByDimension.containsKey(dimension))
			return new ArrayList<Attribute>();
		else
			
			return attributesByDimension.get(dimension);
	}
	
	/**
	 * 
	 * @param attType the subclass of attribute to look for.
	 * @return all attributes with the given type
	 */
	public  ArrayList<Attribute> getAttributesByType(Class<? extends Attribute> attType){
		if (!attributesByType.containsKey(attType))
			return new ArrayList<Attribute>();
		else
			
			return attributesByType.get(attType);	
	}
	
	
	// CONSTRUCTOR
	
	public Table(String name) {
		this.name=name;
		attributes = new ArrayList<Attribute>();
		relationships = new ArrayList<Relationship>();
		primaryKey = new ArrayList<Attribute>();
		attributeByName = new 	HashMap<String,Attribute>();
		attributesByDimension = new HashMap<String,ArrayList<Attribute>>();
		attributesByType = new HashMap<Class<? extends Attribute>,ArrayList<Attribute>>();
	}

	/**
	 * Add an attribute to the table.
	 * @param att the attribute to add.
	 * @throws Exception if an attribute with the same name is already present.
	 */
	public void addAttribute(Attribute att) throws Exception {
		if (attributeByName.containsKey(att.getAttributeName()))
			throw new Exception("Attribute " + att.getAttributeName() + " already present in table " + name);
		
		String attName = att.getAttributeName();
		String attDim = att.getDimension();
		Class<? extends Attribute> attClass = att.getClass();
		attributes.add(att);
		
		// name
		attributeByName.put(attName, att);

		// dim
		if (att.getClass() == NominalAttribute.class || att.getClass() == NumericAttribute.class) {
			if (!attributesByDimension.containsKey(attDim))
				attributesByDimension.put(attDim, new ArrayList<Attribute>());
			attributesByDimension.get(attDim).add(att);
		}
		
		// type
		if (!attributesByType.containsKey(attClass))
			attributesByType.put(attClass, new ArrayList<Attribute>());
		attributesByType.get(attClass).add(att);
	}
	
	/**
	 * Defines a single-attribute primary key.
	 * @param att
	 * @throws Exception 
	 */
	public void setPrimaryKey(Attribute att) throws Exception {
		ArrayList<Attribute> attributesList = new ArrayList<>();
		attributesList.add(att);
		setPrimaryKey(attributesList);
	}
	
	/**
	 * Defines a multiple-attribute primary key.
	 * @param attributesList
	 * @throws Exception if trying to set a non-ID attribute as part of the primary key 
     */
	public void setPrimaryKey(ArrayList<Attribute> attributesList ) throws Exception  {
		this.primaryKey = new ArrayList<>();
		for (Attribute att : attributesList) {
//			if (att.getClass() != IDAttribute.class)
//				throw new Exception("Attribute " + att.getAttributeName() + " cannot be part of a primary key because it is not of type ID");
			primaryKey.add(att);
		}
	}

	/**
	 * adds an outgoing relationship to the table.
	 * @param rel 
	 * @throws Exception if the table of the left in rel is not this table.
	 */
	public void addRelationship(Relationship rel) throws Exception{
		if (rel.getTable1() != this)
			throw new Exception ("Table1 is not set correctly");
		this.relationships.add(rel);
	}

	public void setRowSample(HashMap<String,HashMap<String,Integer>> rowSample) {
		this.rowSample = rowSample;
	}

	public HashMap<String,HashMap<String, Integer>> getRowSample() {
		return rowSample;
	}
}
