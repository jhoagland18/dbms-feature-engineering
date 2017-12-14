package core_package.Schema;

import java.util.ArrayList;


public class Relationship {

	private Table table1;
	private Table table2;
	private ArrayList<IDAttribute> attributes1;
	private ArrayList<IDAttribute> attributes2;
	private RelationshipType type;
	
	/**
	 * 
	 * @return the table on the left
	 */
	public Table getTable1() {return table1;}
	
	/**
	 * 
	 * @return the table on the right
	 */
	public Table getTable2() {return table2;}
	
	/**
	 * 
	 * @return the attributes of table 1 used for the join
	 */
	public ArrayList<IDAttribute> getAttributes1() {return attributes1;}

	/**
	 * 
	 * @return the attributes of table 2 used for the join
	 */
	public ArrayList<IDAttribute> getAttributes2() {return attributes2;}
	
	/**
	 * 
	 * @return the relationship type: table 1 is in a (to1 or toN) relationship with table 2
	 */
	public RelationshipType getRelationshipType() {return type;}
	
	/**
	 * 
	 * @param table1 the table on the "left" side
	 * @param table2 the table on the "right" side
	 * @param attributes1 the keys used for the joins in table1
	 * @param attributes2 the keys used for the joins in table2
	 * @param relType the relationship type
	 */
	public Relationship (Table table1, Table table2, ArrayList<IDAttribute> attributes1,ArrayList<IDAttribute> attributes2, RelationshipType relType) {
		this.attributes1=attributes1;
		this.attributes2=attributes2;

		this.table1 = table1;
		this.table2 = table2;
		
		this.type = relType;
	}

}
