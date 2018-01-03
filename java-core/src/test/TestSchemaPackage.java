package test;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import core_package.Schema.*;

import java.time.Period;
import java.util.ArrayList;

class TestSchemaPackage {

	@BeforeEach
	void setUp() throws Exception {
	}

	@AfterEach
	void tearDown() throws Exception {
	}

	@Test
	void oneTableTest() throws Exception {
		// PURCHASES
		Table purchases = new Table("Purchases");
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
	}

	@Test
	void wholeDB() throws Exception {
		// PURCHASES
		Table purchases = new Table("Purchases");
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
}

}
