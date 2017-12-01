package core_package.Schema;

import core_package.Main;
import core_package.Schema.Attribute.Attribute;
import core_package.Schema.Attribute.ForeignKeyCategoricalAttribute;
import core_package.Schema.Attribute.ForeignKeyDateAttribute;
import core_package.Schema.Attribute.ForeignKeyNumericalAttribute;

public class Relationship {

	private Table primaryTable;
	private Table foreignTable;
	private Attribute primaryKey;
	private Attribute foreignKey;

	//store relationship types
	public final static int ONE_TO_MANY=1;
	public final static int ONE_TO_ONE=0;
	
	public Relationship (Attribute att1, Attribute att2, int cardinality) {
		
		if (att1 instanceof ForeignKeyCategoricalAttribute) {
			Table link = ((ForeignKeyCategoricalAttribute)att1).getLink();
			if(link==att2.getParentTable()) {
				foreignTable = att1.getParentTable();
				primaryTable = att2.getParentTable();
				primaryKey = att1;
				foreignKey=att2;
				
			}
		}
		else if (att1 instanceof ForeignKeyDateAttribute) {
			Table link = ((ForeignKeyDateAttribute)att1).getLink();
			if(link==att2.getParentTable()) {
				foreignTable = att1.getParentTable();
				primaryTable = att2.getParentTable();
				primaryKey = att1;
				foreignKey=att2;
			}
		}	
		else if (att1 instanceof ForeignKeyNumericalAttribute) {
			Table link = ((ForeignKeyNumericalAttribute)att1).getLink();
			if(link==att2.getParentTable()) {
				foreignTable = att1.getParentTable();
				primaryTable = att2.getParentTable();
				primaryKey = att1;
				foreignKey=att2;
			}
		}
		
		else if (att2 instanceof ForeignKeyCategoricalAttribute) {
			Table link = ((ForeignKeyCategoricalAttribute)att2).getLink();
			if(link==att1.getParentTable()) {
				foreignTable = att2.getParentTable();
				primaryTable = att1.getParentTable();
				primaryKey = att2;
				foreignKey=att1;
			}
		}
		else if (att2 instanceof ForeignKeyDateAttribute) {
			Table link = ((ForeignKeyDateAttribute)att2).getLink();
			if(link==att1.getParentTable()) {
				foreignTable = att2.getParentTable();
				primaryTable = att1.getParentTable();
				primaryKey = att2;
				foreignKey=att1;
			}
		}	
		else if (att2 instanceof ForeignKeyNumericalAttribute) {
			Table link = ((ForeignKeyNumericalAttribute)att2).getLink();
			if(link==att1.getParentTable()) {
				foreignTable = att2.getParentTable();
				primaryTable = att1.getParentTable();
				primaryKey = att2;
				foreignKey=att1;
			}
		}
		

		Main.printVerbose("att is " + att1.toString());
		Main.printVerbose("table is "+att1.getParentTable());

		addRelationshipsToTables();
	}
	
	public Table[] getTables() {
		Table[] tables = new Table[2];
		tables[0] = primaryTable;
		tables[1] = foreignTable;
		return tables;
	}

	public void addRelationshipsToTables() {
		primaryTable.addRelationship(this);
		foreignTable.addRelationship(this);
	}

	public boolean hasTable(Table t) {
		return (primaryTable==t || foreignTable==t) ? true : false;
	}
	
	public String toString() {
		return primaryTable.toString() + "     " +foreignTable.toString();
	}
	
	public Table getForeignTable() {
		return foreignTable;
	}
	
	public Table getPrimaryTable() {
		return primaryTable;
	}
}
