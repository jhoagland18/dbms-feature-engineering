package core_package.QueryGeneration;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import org.jpl7.Term;


public class QueryBuilder {
	private static Boolean debug = false;
	
	/**
	 * Generates all queries for all templates contained in templateDirectory. The templates are sorted by 
	 * increasing levels of complexity.
	 * @param targetTableName
	 * @param templateDirectory
	 * @return
	 * @throws IOException
	 */
	public static ArrayList<Query> buildQueriesFromDirectory(String targetTableName, String templateDirectory) throws IOException {
		long startTime = System.nanoTime();    
		// ... the code being measured ...    
		int number = 0;
		ArrayList<Query> result = new ArrayList<>();
		File dir = new File(templateDirectory);
		File[] directoryListing = dir.listFiles();
		if (directoryListing != null) {
			for (File child : directoryListing) {
				for (Query q : buildQueries(targetTableName, child.getAbsolutePath())) { 
					result.add(q);
					long estimatedTime = System.nanoTime() - startTime;
					System.out.println("Generated " + (++number) + " queries in " + estimatedTime / 10000000.0 + " ms");
				}
			}
		}
		result.sort(new QueryComparator());
		return result;
	}

	/**
	 * Builds queries from one template file
	 * @param targetTableName the name of the target table, which will be bound to T0
	 * @param templateFile the prolog file relative to the query
	 * @param eng the Prolog engine
	 * @return all queries that can be formed
	 * @throws IOException
	 * @throws MalformedGoalException 
	 * @throws NoSolutionException 
	 * @throws NoMoreSolutionException 
	 */
	public static ArrayList<Query> buildQueries(String targetTableName, String templateFile) throws IOException {
		ArrayList<Query> result = new ArrayList<>();
		BufferedReader br = new BufferedReader(new FileReader(templateFile));
		String code = "T0='"+targetTableName+"',\n";
		String curLine ;
		while ((curLine = br.readLine()) != null) {
				code += curLine + "\n";
		}
		Boolean provable = org.jpl7.Query.hasSolution(code);
//		System.out.println("The code is " + (provable ? "provable" : "not provable"));
//		System.out.println("Code: " + code);
		Map<String, Term>[] ss4 = org.jpl7.Query.allSolutions(code);
		
		for (int i=0;i<ss4.length;i++) {
			Map<String,Term> res =  ss4[i];
			String sql = res.get("Query").toString();
			sql = sql.replaceAll("\\n", "\n");
			String description = res.get("Description").toString();
			Double complexity = Double.parseDouble(res.get("Complexity").toString());
			Query q = new Query(sql,complexity,description);
			result.add(q);
		}

		return result;
	}
}
