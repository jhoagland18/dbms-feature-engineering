package core_package;

import core_package.Pathfinding.Path;
import core_package.Schema.Relationship;
import core_package.Schema.Table;

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
						" LEFT [OUTER] JOIN " + a.getTableName() + " ON " + a.getTableName()+"."+fKey+" = "+ a.getTableName()+"."+pKey;
				queries.add(s);
			}
		}
		return queries;
	}
}
