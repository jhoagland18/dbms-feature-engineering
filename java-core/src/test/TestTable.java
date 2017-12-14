package test;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import core_package.Schema.*;
import junit.framework.Assert;

import java.time.Period;
import java.util.ArrayList;

class TestTable {

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
		purchases.addAttribute(new IDAttribute("PurchaseID", null));
		purchases.addAttribute(new IDAttribute("ClientID",null));
		purchases.addAttribute(new IDAttribute("ProductID", null));
		purchases.addAttribute(new ZeroOneAttribute("Return", null));
		
		ArrayList<Period> periods = new ArrayList<>();
		
		periods.add(Period.ofMonths(1));
		periods.add(Period.ofWeeks(2));
		purchases.addAttribute(new TimeStampAttribute("Date", null,periods));
		purchases.setPrimaryKey(purchases.getAttributeByName("PurchaseID"));
		Assertions.assertEquals(5, purchases.getAttributes().size());
		Assertions.assertEquals(3, purchases.getAttributesByType(IDAttribute.class).size());
		Assertions.assertEquals(1, purchases.getAttributesByType(TimeStampAttribute.class).size());
		Assertions.assertEquals(1, purchases.getAttributesByType(ZeroOneAttribute.class).size());
		Assertions.assertEquals(0, purchases.getAttributesByType(NumericAttribute.class).size());
		Assertions.assertEquals(0, purchases.getAttributesByType(NominalAttribute.class).size());
		Assertions.assertEquals(0, purchases.getAttributesByDimension("").size());
	}

}
