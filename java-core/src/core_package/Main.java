package core_package;

import core_package.Pathfinding.Path;
import core_package.Schema.DBSchema;
import core_package.SchemaMapper.DatabaseConnection;
import core_package.SchemaMapper.SchemaBuilder;
import javax.naming.NameAlreadyBoundException;
import java.util.ArrayList;


//Created by Michele Samorani, Gayatri Krishnan, and Jackson Hoagland during academic research with Santa Clara University on 9/29/2017

public class Main {

	static ArrayList<Path> paths;
	static final int printMode=Environment.PRINT_MODE_VERBOSE; //enable for verbose output logging

	public static
	void main(String args[]) throws NameAlreadyBoundException {

		SchemaBuilder sm = new SchemaBuilder(DatabaseConnection.MICROSOFT_SQL_SERVER);
		try {
			System.out.println("Detected schema:\n"+sm.buildSchema());
		} catch (Exception e) {
			e.printStackTrace();
		}

		DBSchema sc = sm.getSchema();

		paths = new ArrayList<Path>();
/*
		Table purchases = new Table("Purchases");
		purchases.addNumericalAttribute("purchase_id", true);
		purchases.addNumericalAttribute("online", false);
		purchases.addNumericalAttribute("return", false);
		purchases.addDateAttribute("date", false);

		Table products = new Table("Products");
		products.addNumericalAttribute("product_id", true);
		products.addNumericalAttribute("price", false);

		Table clients = new Table("Clients");
		clients.addNumericalAttribute("client_id", true);
		clients.addNumericalAttribute("age", false);
		clients.addCatagoricalAttribute("gender", false);
		
		sc.addTable(purchases);
		sc.addTable(clients);
		sc.addTable(products);
		
		sc.createRelationship(products.getPrimaryKey(), purchases.addNumericalForeignKey("purchase_id", products), Relationship.ONE_TO_ONE);
		sc.createRelationship(clients.getPrimaryKey(), purchases.addNumericalForeignKey("client_id", clients), Relationship.ONE_TO_MANY);

		
		ArrayList<Relationship> purchasesRel = purchases.getRelationships();
		for (Relationship r : purchasesRel ) {
			System.out.println("Purchase relationships: "+r.getTables()[0]+", "+r.getTables()[1]);
		}
		*/
		ArrayList<Path> paths = new ArrayList<Path>();
		
		sc.createPaths(paths, Environment.targetTableName, 3);

		AttributeGenerator ag = new AttributeGenerator();

		System.out.println("number of paths found: " + paths.size());

		for (Path p : paths) {
			System.out.println("Final path: "+p.toString());
		}


/*		System.out.println("\n");
		for (Path p : paths) {
			ArrayList<String> queries = ag.generate(p);
			for (int i = 0; i < queries.size(); i++) {
				System.out.println("query: "+ queries.get(i));
			}
			System.out.println("\n");
		}

*/

		
		
		
		//System.out.println(sc.buildPaths(null, route, 3));
		//System.out.println(sc.createPaths(partial, toReturn, targetTable, max_length);
		//Table featureTable = attributeGenerate (sc, p, "target-attribute");
	}

	public static void printVerbose(String toPrint) {
		if(printMode==Environment.PRINT_MODE_VERBOSE)
			System.out.println(toPrint);
	}

}
