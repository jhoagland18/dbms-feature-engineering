package core_package;

public class Main {
	
	public static void main(String args[]) {
		
		Table p = new Table("name", );
		p.addAttribute("purchase_ID", true, ID);
		Table cl = new Table("name", );
		p.addAttribute("purchase_ID", true, ID);
		
		
		
		DBSchema sc = new DBSchema();
		sc.addTable(p);
		sc.addTable(cl);
		sc.addRelationship(p,cl,"purchaseID", "clientID");
		
		Table featureTable = attributeGenerate (sc, p, "target-attribute");
	}

}
