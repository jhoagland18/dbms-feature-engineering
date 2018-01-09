package core_package;
import core_package.Exception.NoSuchDatabaseTypeException;
import core_package.QueryGeneration.DB2PrologLoader;
import core_package.QueryGeneration.Query;
import core_package.QueryGeneration.QueryBuilder;
import core_package.Schema.*;

import java.util.ArrayList;


import core_package.SchemaBuilder.DatabaseConnection;
import core_package.SchemaBuilder.SchemaBuilder;
import org.jpl7.*;

//Created by Jackson Hoagland, Gayatri Krishnan, and Michele Samorani, during academic research with Santa Clara University on 9/29/2017
 
public class Main {
	static ArrayList<Table> tables = new ArrayList<>();
	static ArrayList<Relationship> relationships = new ArrayList<>();
	
	public static void main (String [] args) throws Exception {

	    try {
            SchemaBuilder sb = new SchemaBuilder(DatabaseConnection.MICROSOFT_SQL_SERVER);
					System.out.println(sb.buildSchema().getSchema().toString());
        } catch (NoSuchDatabaseTypeException e) {
	        e.printStackTrace();
        }

        Schema sc = new Schema();
	    sc.loadTables();

		//loadTables();
		JPL.init();
		
		DB2PrologLoader.LoadDB(
				"prolog/functions.pl",
				sc.getTables(), sc.getRelationships());
		ArrayList<Query> queries= QueryBuilder.buildQueriesFromDirectory("Purchases",
		"prolog/query templates");
		System.out.println("RESULT:");
		for (Query q : queries)
			System.out.println(q.getSQL());
		
		return;
	}

}
