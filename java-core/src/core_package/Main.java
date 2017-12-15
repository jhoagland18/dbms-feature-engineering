package core_package;

import core_package.Pathfinding.*;
import core_package.Schema.*;

import java.util.ArrayList;

//Created by Michele Samorani, Gayatri Krishnan, and Jackson Hoagland during academic research with Santa Clara University on 9/29/2017
 
public class Main {
	public static void main (String [] args) throws Exception {
		Table purchases = new Table("Purchases");
		purchases.addAttribute(new IDAttribute("Purchase_ID"));
		purchases.addAttribute(new IDAttribute("Client_ID"));
		purchases.addAttribute(new IDAttribute("Product_ID"));
		
		Table clients = new Table("Clients");
		clients.addAttribute(new IDAttribute("Client_ID"));
		
		Table products = new Table("Products");
		products.addAttribute(new IDAttribute("Product_ID"));
		
		purchases.addRelationship(new Relationship(purchases, clients, 
				(IDAttribute)purchases.getAttributeByName("Client_ID"), 
				(IDAttribute)clients.getAttributeByName("Client_ID"),RelationshipType.To1));
		clients.addRelationship(new Relationship(clients, purchases, 
				(IDAttribute)clients.getAttributeByName("Client_ID"),
				(IDAttribute)purchases.getAttributeByName("Client_ID"), RelationshipType.ToN));
		purchases.addRelationship(new Relationship(purchases, products, 
				(IDAttribute)purchases.getAttributeByName("Product_ID"), 
				(IDAttribute)products.getAttributeByName("Product_ID"),RelationshipType.To1));
		products.addRelationship(new Relationship(products, purchases, 
				(IDAttribute)products.getAttributeByName("Product_ID"),
				(IDAttribute)purchases.getAttributeByName("Product_ID"),RelationshipType.ToN));
		
		ArrayList<Path> paths = PathGenerator.GeneratePaths(4, purchases);
		for (Path p : paths)
			System.out.println(p.toString());
	}

}
