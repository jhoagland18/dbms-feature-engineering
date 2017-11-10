package core_package;

import java.util.ArrayList;

public class AttributeGenerator {
	private ArrayList<String> queries;
	public AttributeGenerator() {
		queries = new ArrayList<String>();
	}
	
	public ArrayList<String> generate(Path p) {
		
		for (int i = 0; i < p.getRelationships().size(); i++) {
			Table a = p.getRelationship(i).getTables()[0];
			Table b = p.getRelationship(i).getTables()[1];
			for (int j=0; j < a.getAttributes().size(); j++) {
				String fKey="";
				String pKey="";
				if (a.getAttributes().get(j).isFKey()) {
					fKey=a.getAttributes().get(j).getAttributeName();
				}
				if (b.getAttributes().get(j).isPKey()) {
					pKey=a.getAttributes().get(j).getAttributeName();
				}
				String s="SELECT " + a.getAttributes().get(j).getAttributeName() + 
						" FROM " + a.getTableName() + 
						" LEFT [OUTER] JOIN " + b.getTableName() + " ON " + a.getTableName()+"."+fKey+" = "+ b.getTableName()+"."+pKey;
				queries.add(s);
			}
			
		}
		return queries;

	}
}
