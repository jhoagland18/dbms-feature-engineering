package core_package.Schema;

import java.time.Period;
import java.util.ArrayList;

public class Schema {

    static ArrayList<Table> tables = new ArrayList<>();
    static ArrayList<Relationship> relationships = new ArrayList<>();

    public static void loadTables() throws Exception {
        Table purchases = new Table("Purchases");
        purchases.addAttribute(new IDAttribute("Purchase_ID"));
        purchases.setPrimaryKey(new IDAttribute("Purchase_ID"));
        purchases.addAttribute(new IDAttribute("Client_ID"));
        purchases.addAttribute(new IDAttribute("Product_ID"));
        ArrayList<Period> periods = new ArrayList<>();
        periods.add(Period.ofMonths(1));
        periods.add(Period.ofMonths(2));
        periods.add(Period.ofMonths(12));
        purchases.addAttribute(new TimeStampAttribute("date", periods));
        purchases.addAttribute(new ZeroOneAttribute("return"));
        purchases.addAttribute(new ZeroOneAttribute("online"));

        Table clients = new Table("Clients");
        clients.addAttribute(new IDAttribute("Client_ID"));
        clients.setPrimaryKey(new IDAttribute("Client_ID"));
        ArrayList<Double> binsAge = new ArrayList<>();
        binsAge.add(0.0); binsAge.add(20.0); binsAge.add(30.0); binsAge.add(40.0); binsAge.add(50.0);
        binsAge.add(60.0); binsAge.add(100000.0);
        clients.addAttribute(new NumericAttribute("age", "years", binsAge));
        ArrayList<String> gvalues = new ArrayList<>(); gvalues.add("M");gvalues.add("F");
        clients.addAttribute(new NominalAttribute("gender", "gender", gvalues));

        Table products = new Table("Products");
        products.addAttribute(new IDAttribute("Product_ID"));
        products.setPrimaryKey(new IDAttribute("Product_ID"));
        ArrayList<Double> binsPrice = new ArrayList<>();
        binsPrice.add(0.0); binsPrice.add(20.0); binsPrice.add(100.0); binsPrice.add(200.0);binsPrice.add(1000.0); binsPrice.add(1000000.0);
        products.addAttribute(new NumericAttribute("price", "dollars", binsPrice));

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

        tables.add(purchases);
        tables.add(clients);
        tables.add(products);
        for (Table t : tables)
            for (Relationship r : t.getRelationships())
                relationships.add(r);
    }

    public void addTable(Table t) {
        tables.add(t);
    }

    public void addTables(ArrayList<Table> ts) {
        tables.addAll(ts);
    }

    public ArrayList<Table> getTables() {
        return tables;
    }

    public ArrayList<Relationship> getRelationships() {
        return relationships;
    }
}
