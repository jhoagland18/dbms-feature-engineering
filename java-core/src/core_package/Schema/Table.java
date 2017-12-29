package core_package.Schema;

import core_package.Schema.Attribute.*;
import core_package.Main;
import javax.naming.NameAlreadyBoundException;
import java.util.ArrayList;

public class Table {
	private String name;
	private ArrayList<Attribute> attributes;
	private ArrayList<Relationship> relationships;
	private ArrayList<Attribute> foreignKeys;

	private Attribute primaryKey=null;

	public Table(String name) {
		this.name=name;
		attributes = new ArrayList<Attribute>();
		relationships = new ArrayList<Relationship>();
		foreignKeys = new ArrayList<Attribute>();
	}

	public void addAttribute(Attribute a) throws NameAlreadyBoundException {
		if(checkIfAttributeNameAlreadyExists(a.getAttributeName())) {
			throw new NameAlreadyBoundException("An attribute with name " + a.getAttributeName() + " already exists in " + this.getTableName());
		} else {
			attributes.add(a);
			a.setParentTable(this);
		}
	}

	public NumericalAttribute addNumericalAttribute(String attributeName, boolean isPrimaryKey) throws NameAlreadyBoundException {
		if(checkIfAttributeNameAlreadyExists(attributeName)) {
			throw new NameAlreadyBoundException("An attribute with name " + attributeName + " already exists in " + this.getTableName());
		}
		if (isPrimaryKey && primaryKey != null) {
			return null;
		} else {
			NumericalAttribute a = new NumericalAttribute(attributeName, isPrimaryKey, this);
			if (isPrimaryKey) {
				primaryKey = a;
			}
			attributes.add(a);
			return a;
		}
	}

	public CategoricalAttribute addCatagoricalAttribute(String attributeName, boolean isPrimaryKey) throws NameAlreadyBoundException {
		if(checkIfAttributeNameAlreadyExists(attributeName)) {
			throw new NameAlreadyBoundException("An attribute with name " + attributeName + " already exists in " + this.getTableName());
		}
		if(isPrimaryKey && primaryKey!=null) {
			return null;
		} else {
			CategoricalAttribute a = new CategoricalAttribute(attributeName, isPrimaryKey, this);
			if(isPrimaryKey) {
				primaryKey=a;
			}
			attributes.add(a);
			return a;
		}
	}

	public DateAttribute addDateAttribute(String attributeName, boolean isPrimaryKey) throws NameAlreadyBoundException {
		if(checkIfAttributeNameAlreadyExists(attributeName)) {
			throw new NameAlreadyBoundException("An attribute with name " + attributeName + " already exists in " + this.getTableName());
		}
		if(isPrimaryKey && primaryKey!=null) {
			return null;
		} else {
			DateAttribute a = new DateAttribute(attributeName, isPrimaryKey, this);
			if(isPrimaryKey) {
				primaryKey=a;
			}
			attributes.add(a);
			return a;
		}
	}
	
	public NumericalAttribute addNumericalForeignKey(String attributeName, Table link) throws NameAlreadyBoundException {
		if(checkIfAttributeNameAlreadyExists(attributeName)) {
			throw new NameAlreadyBoundException("An attribute with name " + attributeName + " already exists in " + this.getTableName());
		}
		ForeignKeyNumericalAttribute a = new ForeignKeyNumericalAttribute(attributeName, this, link);
		foreignKeys.add(a);
		return a;
	}

	public CategoricalAttribute addCategoricalForeignKey(String attributeName, Table link) throws NameAlreadyBoundException {
		if(checkIfAttributeNameAlreadyExists(attributeName)) {
			throw new NameAlreadyBoundException("An attribute with name " + attributeName + " already exists in " + this.getTableName());
		}
		ForeignKeyCategoricalAttribute a = new ForeignKeyCategoricalAttribute(attributeName, this, link);
		foreignKeys.add(a);
		return a;
	}

	public DateAttribute addDateForignKey(String attributeName, Table link) throws NameAlreadyBoundException {
		if(checkIfAttributeNameAlreadyExists(attributeName)) {
			throw new NameAlreadyBoundException("An attribute with name " + attributeName + " already exists in " + this.getTableName());
		}
		ForeignKeyDateAttribute a = new ForeignKeyDateAttribute(attributeName, this, link);
		foreignKeys.add(a);
		return a;
	}
	
	public void addRelationship (Relationship rel) {
		//Relationship rel = new Relationship(targetTable, attribute);
		relationships.add(rel);
		Main.printVerbose("Adding relationship for table: " + name);
	}
	
	public String getTableName() {
		return name;
	}
	
	public boolean isPrimaryKeySet() {
		return primaryKey!=null;
	}
	
	public ArrayList<Relationship> getRelationships () {
		return relationships;
	}
	
	public String toString() {
		String toReturn = name;
		toReturn+= "(PK = "+this.getPrimaryKey()+")\nAttributes:\n";

		for(Attribute a: attributes) {
			toReturn += a.toString()+"\n";
		}

		return toReturn;
	}

	public Attribute getAttribute(String name) {
		for(Attribute a : attributes) {
			if(a.getAttributeName().equals(name))
				return a;
		}
		return null;
	}
	public ArrayList<Attribute> getAttributes() {
		return attributes;
	}
	
	public Attribute getPrimaryKey() {
		return primaryKey;
	}
	
	public ArrayList<Attribute> getForeignKeys() {
		return foreignKeys;
	}

	public boolean checkIfAttributeNameAlreadyExists(String name) {
		for(Attribute a : attributes) {
			if(a.getAttributeName().equals(name))
				return true;
		}
		return false;
	}

	public boolean removeAttribute(Attribute a) {
		return attributes.remove(a);
	}
}
