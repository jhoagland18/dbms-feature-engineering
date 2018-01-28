package core_package;
import core_package.Exception.NoSuchDatabaseTypeException;
import core_package.FeatureSelection.QueryExecutor;
import core_package.FeatureSelection.QueryExecutorController;
import core_package.QueryGeneration.DB2PrologLoader;
import core_package.QueryGeneration.Query;
import core_package.QueryGeneration.QueryBuilder;
import core_package.Schema.*;

import java.time.Period;
import java.util.ArrayList;


import core_package.SchemaBuilder.DatabaseConnection;
import core_package.SchemaBuilder.SchemaBuilder;
import org.jpl7.*;

//Created by Jackson Hoagland, Gayatri Krishnan, and Michele Samorani, during academic research with Santa Clara University on 9/29/2017
 
public class Main {
	static ArrayList<Table> tables = new ArrayList<>();
	static ArrayList<Relationship> relationships = new ArrayList<>();
	
	public static void main (String [] args) throws Exception {
	
//	    try {
//            SchemaBuilder sb = new SchemaBuilder(DatabaseConnection.MICROSOFT_SQL_SERVER);
//					System.out.println(sb.buildSchema().getSchema().toString());
//
//        Schema sc = new Schema();
//	    sc.loadTables();
//	    tables = sc.getTables();
//	    relationships = sc.getRelationships();
//        } catch (Exception e) {
//    		loadTables();
//        }


		//loadDataconda();
		loadCacao();
	    
//	    System.out.println("********************");
//	    System.out.println("ATTRIBUTE GENERATION");
//	    System.out.println("********************");
		JPL.init();

	   System.out.println("Working Directory = " +
	               System.getProperty("user.dir"));

	   System.out.println("Loading database...");
		DB2PrologLoader.LoadDB(
				"prolog/functions.pl",
				tables, relationships);

		System.out.println("Generating feature queries...");

		ArrayList<Query> queries= QueryBuilder.buildQueriesFromDirectory("Ratings",
		"prolog/query templates");

		System.out.println("GENERATED QUERIES:");
		for (Query q : queries)
			System.out.println(q.getSQL());

		System.out.println("Executing queries and comparing correlation to dependant...");

		long startTime = System.nanoTime();
		QueryExecutorController qec = new QueryExecutorController(4, "RatingID","rating", DatabaseConnection.MICROSOFT_SQL_SERVER, queries);
		long elapsedTime = System.nanoTime() - startTime;

		System.out.println("Correlation analysis finished. Elapsed time: "+elapsedTime/1000000000.0);

		System.out.println("Analyzing features...");

		Process featureAnalyzer = Runtime.getRuntime().exec("py python-core/FeatureAnalyzer/featureAnalysis.py");

		featureAnalyzer.waitFor();

		System.out.println("Generating report web page...");

		Process reportGenerator = Runtime.getRuntime().exec("py	" +
				" python-core/ReportGenerator/reportGenerator.py");

		System.out.println("Done.");

		return;
	}

	private static void loadCacao() throws Exception {
		Table cacao = new Table("CacaoBar");
		cacao.addAttribute(new IDAttribute("cacaobarid"));
		cacao.setPrimaryKey(cacao.getAttributeByName("cacaobarid"));

		ArrayList<Double> cacaoBins = new ArrayList<>();
		cacaoBins.add(0.71);
		cacaoBins.add(0.66);
		cacaoBins.add(0.55);
		cacaoBins.add(0.5);
		cacaoBins.add(0.63);
		cacaoBins.add(0.64);
		cacaoBins.add(0.85);
		cacaoBins.add(0.61);
		cacaoBins.add(0.78);
		cacaoBins.add(0.62);
		cacaoBins.add(0.77);
		cacaoBins.add(0.75);
		cacaoBins.add(0.73);
		cacaoBins.add(0.69);
		cacaoBins.add(0.76);
		cacaoBins.add(0.7);
		cacaoBins.add(0.65);
		cacaoBins.add(0.58);
		cacaoBins.add(0.735);
		cacaoBins.add(0.84);
		cacaoBins.add(0.74);
		cacaoBins.add(0.67);
		cacaoBins.add(0.42);
		cacaoBins.add(0.8);
		cacaoBins.add(0.91);
		cacaoBins.add(0.57);
		cacaoBins.add(0.53);
		cacaoBins.add(0.9);
		cacaoBins.add(0.88);
		cacaoBins.add(0.6);
		cacaoBins.add(0.86);
		cacaoBins.add(0.89);
		cacaoBins.add(0.72);
		cacaoBins.add(0.605);
		cacaoBins.add(0.87);
		cacaoBins.add(0.81);
		cacaoBins.add(1.0);
		cacaoBins.add(0.725);
		cacaoBins.add(0.82);
		cacaoBins.add(0.68);
		cacaoBins.add(0.46);
		cacaoBins.add(0.83);
		cacaoBins.add(0.99);
		cacaoBins.add(0.56);
		cacaoBins.add(0.79);

		cacao.addAttribute(new NumericAttribute("Cocoa_Percent","percent",cacaoBins));

		ArrayList<String> beanTypeImpValues = new ArrayList<>();
		beanTypeImpValues.add("Nacional");
		beanTypeImpValues.add("Criollo");
		beanTypeImpValues.add(" Forastero");
		beanTypeImpValues.add("Forastero (Nacional)");
		beanTypeImpValues.add("Trinitario");
		beanTypeImpValues.add(" Forastero");
		beanTypeImpValues.add("Criollo (Amarru)");
		beanTypeImpValues.add("Trinitario");
		beanTypeImpValues.add(" TCGA");
		beanTypeImpValues.add("Criollo");
		beanTypeImpValues.add("Blend");
		beanTypeImpValues.add("Forastero (Parazinho)");
		beanTypeImpValues.add("Trinitario");
		beanTypeImpValues.add(" Criollo");
		beanTypeImpValues.add("Forastero(Arriba");
		beanTypeImpValues.add(" CCN)");
		beanTypeImpValues.add("CCN51");
		beanTypeImpValues.add("Criollo (Ocumare 61)");
		beanTypeImpValues.add("Criollo (Ocumare 67)");
		beanTypeImpValues.add("Trinitario");
		beanTypeImpValues.add("Criollo (Ocumare 77)");
		beanTypeImpValues.add("Blend-Forastero");
		beanTypeImpValues.add("Criollo");
		beanTypeImpValues.add("Criollo (Ocumare)");
		beanTypeImpValues.add("Criollo (Porcelana)");
		beanTypeImpValues.add("Criollo");
		beanTypeImpValues.add(" +");
		beanTypeImpValues.add("Forastero (Arriba) ASSS");
		beanTypeImpValues.add("Forastero");
		beanTypeImpValues.add("Matina");
		beanTypeImpValues.add("Amazon mix");
		beanTypeImpValues.add(" ");
		beanTypeImpValues.add("Forastero (Arriba)");
		beanTypeImpValues.add("Forastero (Arriba) ASS");
		beanTypeImpValues.add("Trinitario (85% Criollo)");
		beanTypeImpValues.add("");
		beanTypeImpValues.add("Amazon");
		beanTypeImpValues.add("Forastero (Catongo)");
		beanTypeImpValues.add("Criollo (Wild)");
		beanTypeImpValues.add("Forastero");
		beanTypeImpValues.add(" Trinitario");
		beanTypeImpValues.add("Criollo");
		beanTypeImpValues.add(" Trinitario");
		beanTypeImpValues.add("Beniano");
		beanTypeImpValues.add("Amazon");
		beanTypeImpValues.add(" ICS");
		beanTypeImpValues.add("EET");
		beanTypeImpValues.add("Trinitario (Scavina)");
		beanTypeImpValues.add("Forastero (Amelonado)");
		beanTypeImpValues.add("Nacional (Arriba)");
		beanTypeImpValues.add("Trinitario (Amelonado)");
		beanTypeImpValues.add("Trinitario");
		beanTypeImpValues.add(" Nacional");

		cacao.addAttribute(new NominalAttribute("Bean_Type","",beanTypeImpValues));

		ArrayList<String> beanOrigin = new ArrayList<>();
		beanOrigin.add("Africa, Carribean, C. Am.");
		beanOrigin.add("Australia");
		beanOrigin.add("Belize");
		beanOrigin.add("Bolivia");
		beanOrigin.add("Brazil");
		beanOrigin.add("Burma");
		beanOrigin.add("Cameroon");
		beanOrigin.add("Carribean");
		beanOrigin.add("Carribean(DR/Jam/Tri)");
		beanOrigin.add("Central and S. America");
		beanOrigin.add("Colombia");
		beanOrigin.add("Colombia, Ecuador");
		beanOrigin.add("Congo");
		beanOrigin.add("Cost Rica, Ven");
		beanOrigin.add("Costa Rica");
		beanOrigin.add("Cuba");
		beanOrigin.add("Dom. Rep., Madagascar");
		beanOrigin.add("Domincan Republic");
		beanOrigin.add("Dominican Rep., Bali");
		beanOrigin.add("Dominican Republic");
		beanOrigin.add("DR, Ecuador, Peru");
		beanOrigin.add("Ecuador");
		beanOrigin.add("Ecuador, Costa Rica");
		beanOrigin.add("Ecuador, Mad., PNG");
		beanOrigin.add("El Salvador");
		beanOrigin.add("Fiji");
		beanOrigin.add("Gabon");
		beanOrigin.add("Ghana");
		beanOrigin.add("Ghana & Madagascar");
		beanOrigin.add("Ghana, Domin. Rep");
		beanOrigin.add("Ghana, Panama, Ecuador");
		beanOrigin.add("Gre., PNG, Haw., Haiti, Mad");
		beanOrigin.add("Grenada");
		beanOrigin.add("Guat., D.R., Peru, Mad., PNG");
		beanOrigin.add("Guatemala");
		beanOrigin.add("Haiti");
		beanOrigin.add("Hawaii");
		beanOrigin.add("Honduras");
		beanOrigin.add("India");
		beanOrigin.add("Indonesia");
		beanOrigin.add("Indonesia, Ghana");
		beanOrigin.add("Ivory Coast");
		beanOrigin.add("Jamaica");
		beanOrigin.add("Liberia");
		beanOrigin.add("Mad., Java, PNG");
		beanOrigin.add("Madagascar");
		beanOrigin.add("Madagascar & Ecuador");
		beanOrigin.add("Malaysia");
		beanOrigin.add("Martinique");
		beanOrigin.add("Mexico");
		beanOrigin.add("Nicaragua");
		beanOrigin.add("Nigeria");
		beanOrigin.add("Panama");
		beanOrigin.add("Papua New Guinea");
		beanOrigin.add("Peru");
		beanOrigin.add("Peru(SMartin,Pangoa,nacional)");
		beanOrigin.add("Peru, Belize");
		beanOrigin.add("Peru, Dom. Rep");
		beanOrigin.add("Peru, Ecuador");
		beanOrigin.add("Peru, Ecuador, Venezuela");
		beanOrigin.add("Peru, Mad., Dom. Rep.");
		beanOrigin.add("Peru, Madagascar");
		beanOrigin.add("Philippines");
		beanOrigin.add("PNG, Vanuatu, Mad");
		beanOrigin.add("Principe");
		beanOrigin.add("Puerto Rico");
		beanOrigin.add("Samoa");
		beanOrigin.add("Sao Tome");
		beanOrigin.add("Sao Tome & Principe");
		beanOrigin.add("Solomon Islands");
		beanOrigin.add("South America");
		beanOrigin.add("South America, Africa");
		beanOrigin.add("Sri Lanka");
		beanOrigin.add("St. Lucia");
		beanOrigin.add("Suriname");
		beanOrigin.add("Tanzania");
		beanOrigin.add("Tobago");
		beanOrigin.add("Togo");
		beanOrigin.add("Trinidad");
		beanOrigin.add("Trinidad, Ecuador");
		beanOrigin.add("Trinidad, Tobago");
		beanOrigin.add("Trinidad-Tobago");
		beanOrigin.add("Uganda");
		beanOrigin.add("Vanuatu");
		beanOrigin.add("Ven, Bolivia, D.R.");
		beanOrigin.add("Ven, Trinidad, Ecuador");
		beanOrigin.add("Ven., Indonesia, Ecuad.");
		beanOrigin.add("Ven., Trinidad, Mad.");
		beanOrigin.add("Ven.,Ecu.,Peru,Nic.");
		beanOrigin.add("Venez,Africa,Brasil,Peru,Mex");
		beanOrigin.add("Venezuela");
		beanOrigin.add("Venezuela, Carribean");
		beanOrigin.add("Venezuela, Dom. Rep.");
		beanOrigin.add("Venezuela, Ghana");
		beanOrigin.add("Venezuela, Java");
		beanOrigin.add("Venezuela, Trinidad");
		beanOrigin.add("Venezuela/ Ghana");
		beanOrigin.add("Vietnam");
		beanOrigin.add("West Africa");
		cacao.addAttribute(new NominalAttribute("Broad Bean_Origin","",beanOrigin));

		cacao.addAttribute(new IDAttribute("CompanyID"));

		Table company = new Table("Company");
		company.addAttribute(new IDAttribute("companyid"));
		company.setPrimaryKey(company.getAttributeByName("companyid"));

		ArrayList<String> companyLocationImpValues = new ArrayList<>();

		companyLocationImpValues.add("Amsterdam");
		companyLocationImpValues.add("Argentina");
		companyLocationImpValues.add("Australia");
		companyLocationImpValues.add("Austria");
		companyLocationImpValues.add("Belgium");
		companyLocationImpValues.add("Bolivia");
		companyLocationImpValues.add("Brazil");
		companyLocationImpValues.add("Canada");
		companyLocationImpValues.add("Chile");
		companyLocationImpValues.add("Colombia");
		companyLocationImpValues.add("Costa Rica");
		companyLocationImpValues.add("Czech Republic");
		companyLocationImpValues.add("Denmark");
		companyLocationImpValues.add("Domincan Republic");
		companyLocationImpValues.add("Ecuador");
		companyLocationImpValues.add("Eucador");
		companyLocationImpValues.add("Fiji");
		companyLocationImpValues.add("Finland");
		companyLocationImpValues.add("France");
		companyLocationImpValues.add("Germany");
		companyLocationImpValues.add("Ghana");
		companyLocationImpValues.add("Grenada");
		companyLocationImpValues.add("Guatemala");
		companyLocationImpValues.add("Honduras");
		companyLocationImpValues.add("Hungary");
		companyLocationImpValues.add("Iceland");
		companyLocationImpValues.add("India");
		companyLocationImpValues.add("Ireland");
		companyLocationImpValues.add("Israel");
		companyLocationImpValues.add("Italy");
		companyLocationImpValues.add("Japan");
		companyLocationImpValues.add("Lithuania");
		companyLocationImpValues.add("Madagascar");
		companyLocationImpValues.add("Martinique");
		companyLocationImpValues.add("Mexico");
		companyLocationImpValues.add("Netherlands");
		companyLocationImpValues.add("New Zealand");
		companyLocationImpValues.add("Niacragua");
		companyLocationImpValues.add("Nicaragua");
		companyLocationImpValues.add("Peru");
		companyLocationImpValues.add("Philippines");
		companyLocationImpValues.add("Poland");
		companyLocationImpValues.add("Portugal");
		companyLocationImpValues.add("Puerto Rico");
		companyLocationImpValues.add("Russia");
		companyLocationImpValues.add("Sao Tome");
		companyLocationImpValues.add("Scotland");
		companyLocationImpValues.add("Singapore");
		companyLocationImpValues.add("South Africa");
		companyLocationImpValues.add("South Korea");
		companyLocationImpValues.add("Spain");
		companyLocationImpValues.add("St. Lucia");
		companyLocationImpValues.add("Suriname");
		companyLocationImpValues.add("Sweden");
		companyLocationImpValues.add("Switzerland");
		companyLocationImpValues.add("U.K.");
		companyLocationImpValues.add("U.S.A.");
		companyLocationImpValues.add("Venezuela");
		companyLocationImpValues.add("Vietnam");
		companyLocationImpValues.add("Wales");
		company.addAttribute(new NominalAttribute("company_location", "", companyLocationImpValues));

		ArrayList<String> companyNameImpValues = new ArrayList<>();
		companyNameImpValues.add("Soma");
		companyNameImpValues.add("Bonnat");
		companyNameImpValues.add("Fresco");
		companyNameImpValues.add("Pralus");
		companyNameImpValues.add("A. Morin");
		companyNameImpValues.add("Arete");
		companyNameImpValues.add("Domori");
		companyNameImpValues.add("Guittard");
		companyNameImpValues.add("Valrhona");
		companyNameImpValues.add("Hotel Chocolat (Coppeneur)");
		companyNameImpValues.add("Coppeneur");
		companyNameImpValues.add("Mast Brothers");
		companyNameImpValues.add("Scharffen Berger");
		companyNameImpValues.add("Zotter");
		companyNameImpValues.add("Artisan du Chocolat");
		companyNameImpValues.add("Dandelion");
		companyNameImpValues.add("Rogue");
		companyNameImpValues.add("Smooth Chocolator");
		companyNameImpValues.add(" The");
		companyNameImpValues.add("Szanto Tibor");
		companyNameImpValues.add("Bittersweet Origins");
		companyNameImpValues.add("Castronovo");
		companyNameImpValues.add("Pierre Marcolini");
		companyNameImpValues.add("Tejas");
		companyNameImpValues.add("Amedei");
		companyNameImpValues.add("Dick Taylor");
		companyNameImpValues.add("Duffy's");
		companyNameImpValues.add("Pacari");
		companyNameImpValues.add("Friis Holm (Bonnat)");
		companyNameImpValues.add("Madre");
		companyNameImpValues.add("Middlebury");
		companyNameImpValues.add("Palette de Bine");
		companyNameImpValues.add("Sirene");
		companyNameImpValues.add("Altus aka Cao Artisan");
		companyNameImpValues.add("French Broad");
		companyNameImpValues.add("Idilio (Felchlin)");
		companyNameImpValues.add("La Maison du Chocolat (Valrhona)");
		companyNameImpValues.add("Laia aka Chat-Noir");
		companyNameImpValues.add("Map Chocolate");
		companyNameImpValues.add("Marou");
		companyNameImpValues.add("Michel Cluizel");
		company.addAttribute(new NominalAttribute("Company", "", companyLocationImpValues));

		Table locations = new Table("locations");
		locations.addAttribute(new IDAttribute("Company_Location"));
		locations.setPrimaryKey(locations.getAttributeByName("Company_Location"));


		Table ratings = new Table("Ratings");
		ratings.addAttribute(new IDAttribute("cacaobarid"));

		ArrayList<Double> reviewDateImpValues = new ArrayList<>();
		reviewDateImpValues.add(2015.0);
		reviewDateImpValues.add(2011.0);
		reviewDateImpValues.add(2008.0);
		reviewDateImpValues.add(2010.0);
		reviewDateImpValues.add(2007.0);
		reviewDateImpValues.add(2014.0);
		reviewDateImpValues.add(2017.0);
		reviewDateImpValues.add(2009.0);
		reviewDateImpValues.add(2006.0);
		reviewDateImpValues.add(2013.0);
		reviewDateImpValues.add(2012.0);
		reviewDateImpValues.add(2016.0);

		ratings.addAttribute(new NumericAttribute("Review_Date", "", reviewDateImpValues));

		ArrayList<Double> ratingImpValues = new ArrayList<>();
		ratingImpValues.add(2.75);
		ratingImpValues.add(3.0);
		ratingImpValues.add(3.25);
		ratingImpValues.add(3.5);
		ratingImpValues.add(1.5);
		ratingImpValues.add(3.75);
		ratingImpValues.add(4.0);
		ratingImpValues.add(1.75);
		ratingImpValues.add(1.0);
		ratingImpValues.add(2.25);
		ratingImpValues.add(5.0);
		ratingImpValues.add(2.5);
		ratingImpValues.add(2.0);
		ratings.addAttribute(new NumericAttribute("Rating", "", ratingImpValues));
		ratings.addAttribute(new IDAttribute("RatingID"));
		ratings.setPrimaryKey(ratings.getAttributeByName("RatingID"));

        locations.addRelationship(new Relationship(locations, company, (IDAttribute)locations.getAttributeByName("Company_Locations"),(IDAttribute)company.getAttributeByName("companyid"),RelationshipType.ToN)); //one location can have many companies
		company.addRelationship(new Relationship(company, cacao, (IDAttribute)company.getAttributeByName("companyid"),(IDAttribute)cacao.getAttributeByName("cacaobarid"),RelationshipType.ToN)); //one oompany can have many cacaos
        cacao.addRelationship(new Relationship(cacao, ratings, (IDAttribute)cacao.getAttributeByName("cacaobarid"),(IDAttribute)ratings.getAttributeByName("RatingID"),RelationshipType.ToN)); //one cacao can have many ratings

		tables.add(cacao);
		tables.add(ratings);
		tables.add(company);
		tables.add(locations);

		for (Table t : tables)
			for (Relationship r : t.getRelationships())
				relationships.add(r);
	}


	private static void loadDataconda() throws Exception {
		Table purchases = new Table("Purchases");
		purchases.addAttribute(new IDAttribute("Purchase_ID"));
		purchases.setPrimaryKey(purchases.getAttributeByName("Purchase_ID"));
		purchases.addAttribute(new IDAttribute("Client_ID"));
		purchases.addAttribute(new IDAttribute("Product_ID"));
		ArrayList<Period> periods = new ArrayList<>();
		periods.add(Period.ofMonths(1));
		periods.add(Period.ofMonths(2));
		periods.add(Period.ofMonths(12));
		purchases.addAttribute(new TimeStampAttribute("date", periods));
		purchases.addAttribute(new ZeroOneAttribute("returned"));
		purchases.addAttribute(new ZeroOneAttribute("online"));

		Table clients = new Table("Clients");
		clients.addAttribute(new IDAttribute("Client_ID"));
		clients.setPrimaryKey(clients.getAttributeByName("Client_ID"));
		ArrayList<Double> binsAge = new ArrayList<>();
		binsAge.add(0.0);
		binsAge.add(20.0);
		binsAge.add(30.0);
		binsAge.add(40.0);
		binsAge.add(50.0);
		binsAge.add(60.0);
		binsAge.add(100000.0);
		clients.addAttribute(new NumericAttribute("age", "years", binsAge));
		ArrayList<String> gvalues = new ArrayList<>();
		gvalues.add("M");
		gvalues.add("F");
		clients.addAttribute(new NominalAttribute("gender", "gender", gvalues));

		Table products = new Table("Products");
		products.addAttribute(new IDAttribute("Product_ID"));
		products.setPrimaryKey(products.getAttributeByName("Product_ID"));
		ArrayList<Double> binsPrice = new ArrayList<>();
		binsPrice.add(0.0);
		binsPrice.add(20.0);
		binsPrice.add(100.0);
		binsPrice.add(200.0);
		binsPrice.add(1000.0);
		binsPrice.add(1000000.0);
		products.addAttribute(new NumericAttribute("price", "dollars", binsPrice));

		purchases.addRelationship(
				new Relationship(purchases, clients, (IDAttribute) purchases.getAttributeByName("Client_ID"),
						(IDAttribute) clients.getAttributeByName("Client_ID"), RelationshipType.To1));
		clients.addRelationship(
				new Relationship(clients, purchases, (IDAttribute) clients.getAttributeByName("Client_ID"),
						(IDAttribute) purchases.getAttributeByName("Client_ID"), RelationshipType.ToN));
		purchases.addRelationship(
				new Relationship(purchases, products, (IDAttribute) purchases.getAttributeByName("Product_ID"),
						(IDAttribute) products.getAttributeByName("Product_ID"), RelationshipType.To1));
		products.addRelationship(
				new Relationship(products, purchases, (IDAttribute) products.getAttributeByName("Product_ID"),
						(IDAttribute) purchases.getAttributeByName("Product_ID"), RelationshipType.ToN));

		tables.add(purchases);
		tables.add(clients);
		tables.add(products);
		for (Table t : tables)
			for (Relationship r : t.getRelationships())
				relationships.add(r);
	}
}
