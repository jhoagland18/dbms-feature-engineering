package core_package;

//Created by Gayatri Krishnan and Jackson Hoagland on 9/29/2017

public class Main {
	
	public static void main(String args[]) {
		
		Table p = new Table("name"); 
		p.addAttribute("purchase_ID", true);
		Table cl = new Table("name");
		p.addAttribute("purchase_ID", true);
		
		
		
		DBSchema sc = new DBSchema();
		sc.addTable(p);
		sc.addTable(cl);
		sc.addRelationship(p,cl,"purchaseID", "clientID");
		
		Table featureTable = attributeGenerate (sc, p, "target-attribute");
	}

}
