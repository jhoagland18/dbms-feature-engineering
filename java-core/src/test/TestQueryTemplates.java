package test;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Period;
import java.util.ArrayList;
import java.util.HashMap;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import core_package.Schema.Attribute;
import core_package.Schema.IDAttribute;
import core_package.Schema.NominalAttribute;
import core_package.Schema.NumericAttribute;
import core_package.Schema.Relationship;
import core_package.Schema.RelationshipType;
import core_package.Schema.Table;
import core_package.Schema.TimeStampAttribute;
import core_package.Schema.ZeroOneAttribute;
import core_package.Pathfinding.Path;
import core_package.QueryTemplates.*;

class TestQueryTemplates {

	Table purchases;
	Path p;
	
	@BeforeEach
	void setUp() throws Exception {
		// PURCHASES
		purchases = new Table("Purchases");
		purchases.addAttribute(new IDAttribute("PurchaseID"));
		purchases.addAttribute(new IDAttribute("ClientID"));
		purchases.addAttribute(new IDAttribute("ProductID"));
		purchases.addAttribute(new ZeroOneAttribute("Return"));
		
		ArrayList<Period> periods = new ArrayList<>();
		
		periods.add(Period.ofMonths(1));
		periods.add(Period.ofWeeks(2));
		purchases.addAttribute(new TimeStampAttribute("Date",periods));
		purchases.setPrimaryKey(purchases.getAttributeByName("PurchaseID"));
		Assertions.assertEquals(5, purchases.getAttributes().size());
		Assertions.assertEquals(3, purchases.getAttributesByType(IDAttribute.class).size());
		Assertions.assertEquals(1, purchases.getAttributesByType(TimeStampAttribute.class).size());
		Assertions.assertEquals(1, purchases.getAttributesByType(ZeroOneAttribute.class).size());
		Assertions.assertEquals(0, purchases.getAttributesByType(NumericAttribute.class).size());
		Assertions.assertEquals(0, purchases.getAttributesByType(NominalAttribute.class).size());
		Assertions.assertEquals(0, purchases.getAttributesByDimension("").size());
		
		// CLIENTS
		Table clients = new Table("Clients");
		clients.addAttribute(new IDAttribute("ClientID"));
		ArrayList<String> genders = new ArrayList<>();
		genders.add("M"); genders.add("F");
		clients.addAttribute(new NominalAttribute("gender","gender", genders));
		
		ArrayList<Double> ages = new ArrayList<>();
		ages.add(20.0);ages.add(30.0);ages.add(40.0);ages.add(50.0);ages.add(60.0);
		clients.addAttribute(new NumericAttribute("age", "years", ages));
		
		// PRODUCTS
		Table products = new Table("Products");
		products.addAttribute(new IDAttribute("ProductID"));
		ArrayList<Double> prices = new ArrayList<>();
		prices.add(100.0);prices.add(500.0);prices.add(1000.0);
		products.addAttribute(new NumericAttribute("price", "dollars", prices));

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
		
		p = new Path(purchases);
		p.addRelationship(purchases.getRelationships().get(0)); // PU -> CL
		p.addRelationship(p.getHead().getRelationships().get(0)); // PU -> CL -> PU
			}

	@AfterEach
	void tearDown() throws Exception {
	}

	@Test
	void testPathMatch() {
		PathConstraints c1 = new PathConstraints();
		c1.addRelationship(RelationshipType.To1);
		c1.addRelationship(RelationshipType.ToN);		
		Assertions.assertEquals(true, c1.matches(p));
		
		c1 = new PathConstraints();
		c1.addRelationship(RelationshipType.To1);
		c1.addRelationship(RelationshipType.ToN);		
		c1.addRelationship(RelationshipType.To1);		
		Assertions.assertEquals(false, c1.matches(p));

		c1 = new PathConstraints();
		c1.addRelationship(RelationshipType.To1);
		c1.addRelationship(RelationshipType.To1);		
		Assertions.assertEquals(false, c1.matches(p));
		
		c1 = new PathConstraints();
		c1.addRelationship(RelationshipType.To1);
		c1.addRelationship(RelationshipType.ToN);
		c1.addDifferentFromConstraint(0, 2);
		Assertions.assertEquals(false, c1.matches(p));
		
		c1 = new PathConstraints();
		c1.addRelationship(RelationshipType.To1);
		c1.addRelationship(RelationshipType.ToN);
		c1.addEqualToConstraint(0, 2);
		Assertions.assertEquals(true, c1.matches(p));
		
		c1 = new PathConstraints();
		c1.addRelationship(RelationshipType.To1);
		c1.addRelationship(RelationshipType.ToN);
		c1.addDifferentFromConstraint(0, 1);
		Assertions.assertEquals(true, c1.matches(p));
	}
	
	@Test
	void testPKTransformation() {
		PK_FunctionTransformer tr = new PK_FunctionTransformer();
		String original = "PK(T0,t)";
		HashMap <String, Table> tableDict = new HashMap<>();
		tableDict.put("T0", purchases);
		HashMap <String, Attribute> attributeDict = new HashMap<>();
		String res = tr.Transform(original, tableDict, attributeDict, p);
		Assertions.assertEquals("t.PurchaseID", res);
		
		original = "PK ( T0 , t )";
		res = tr.Transform(original, tableDict, attributeDict, p);
		Assertions.assertEquals("t.PurchaseID", res);
	}
}
