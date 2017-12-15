package test;


import java.util.ArrayList;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import core_package.Pathfinding.Path;
import core_package.Pathfinding.PathGenerator;
import core_package.Schema.IDAttribute;
import core_package.Schema.Relationship;
import core_package.Schema.RelationshipType;
import core_package.Schema.Table;

class TestPaths {

	@BeforeEach
	void setUp() throws Exception {
	}

	@AfterEach
	void tearDown() throws Exception {
	}

	@Test
	void testPaths() throws Exception {
		Table purchases = new Table("Purchases");
		purchases.addAttribute(new IDAttribute("Purchase_ID", purchases));
		purchases.addAttribute(new IDAttribute("Client_ID", purchases));
		purchases.addAttribute(new IDAttribute("Product_ID", purchases));
		
		Table clients = new Table("Clients");
		clients.addAttribute(new IDAttribute("Client_ID", clients));
		
		Table products = new Table("Products");
		products.addAttribute(new IDAttribute("Product_ID", products));
		
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
		
		Assertions.assertEquals(6,paths.size());
	}
}
