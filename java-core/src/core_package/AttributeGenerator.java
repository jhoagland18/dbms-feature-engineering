package core_package;

import core_package.Pathfinding.Path;
import core_package.Schema.Relationship;
import core_package.Schema.Table;
import core_package.Schema.Attribute.Attribute;

import java.util.ArrayList;

public class AttributeGenerator {
	private ArrayList<String> queries;
	public AttributeGenerator() {
		queries = new ArrayList<String>();
	}
	
	public ArrayList<String> generate(Path p) {
		
		for (int i = 0; i < p.getRelationships().size(); i++) {
			Table a = p.getRelationship(i).getTables()[1];
			Table b = p.getRelationship(i).getTables()[0];
			
			if (p.getLength() == 1) {
				
			}
			else if (p.getLength() > 1) {
				for (Relationship rel : p.getRelationships()) {
					
				}
			}
			
			String fKey="";
			String pKey="";
			
			System.out.println("Table to add relationship to" + p.getRelationships().get(i).toString());
			for (int j=0; j < a.getAttributes().size(); j++) {
				System.out.println("attribute:" + a.getAttributes().get(j).getAttributeName() + "table: " + a.toString());
				if (a.getAttributes().get(j).isPKey()) {
					pKey=a.getAttributes().get(j).getAttributeName();
				}
				/*if (a.getAttributes().get(j).getAttributeName() = b.getAttributes().get(j).getAttributeName()) {
					System.out.println("FKey is set" + a.getAttributes().get(j));
					fKey=a.getAttributes().get(j).getAttributeName();
				}*/
				
			}
			System.out.println(a.toString() + " "+ a.getForeignKeys().size());
			
			for (int j = 0; j<a.getForeignKeys().size(); j++) {
				fKey = a.getForeignKeys().get(j).getAttributeName();

				String s="SELECT *" + 
						" FROM " + a.getTableName() + 
						" LEFT OUTER JOIN " + a.getTableName() + " ON " + a.getTableName()+"."+fKey+" = "+ b.getTableName()+"."+fKey;
				queries.add(s);
			}
		}
		return queries;

	}
	
	
	public String fillNumericalOneToOneTemplate(Attribute att, String[] accessPoints, Relationship rel) {
		Table a = rel.getPrimaryTable();
		Table b = rel.getForeignTable();
		
		
		StringBuilder template = new StringBuilder ("select " + a.getTableName() + "." + a.getPrimaryKey());
		for (int i = 0; i < Attribute.SQL_NUMERICAL_FUNCTIONS.length; i++) {
			template.append(Attribute.SQL_NUMERICAL_FUNCTIONS[i] + ", (" +att.getAttributeName() + ")");
		}
		template.append("\nfrom " + a.getTableName());
		template.append("\n left outer join " + b.getTableName());
		template.append("\n on " + a.getTableName() + "." + a.getPrimaryKey() + " = " + b.getTableName()
				+ "." + );
		// TODO Auto-generated method stub
	
		return template.toString();
	}
}
