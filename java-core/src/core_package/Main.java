package core_package;

import java.util.ArrayList;

import static java.lang.Thread.yield;

//Created by Michele Samorani, Gayatri Krishnan, and Jackson Hoagland during academic research with Santa Clara University on 9/29/2017

public class Main {

	static ArrayList<Path> paths;
	static final int printMode=Environment.PRINT_MODE_VERBOSE; //enable for verbose output logging

	public static
	void main(String args[]) {

		DBSchema sc = new DBSchema();

		paths = new ArrayList<Path>();

		Table route = new Table("route"); 
		route.addAttribute("route_ID", true, false);
		Table flight = new Table ("flight");
		flight.addAttribute("flight_ID", true, false);
		Table pilot = new Table("pilot");
		pilot.addAttribute("pilot_ID", true, false);
		Table cabin = new Table("cabin");
		cabin.addAttribute("cabin_ID", true, false);
		Table destination = new Table("destination");
		destination.addAttribute("destination_ID",true,false);

		sc.addTable(flight);
		sc.addTable(route);
		sc.addTable(pilot);
		sc.addTable(cabin);
		sc.addTable(destination);
		
		sc.createRelationship(route, flight, route.getAttribute("route_ID"), new Attribute("route_ID", false, true, flight));
		sc.createRelationship(flight, pilot, flight.getAttribute("flight_ID"), new Attribute ("flight_ID", false, true, pilot));
		sc.createRelationship(pilot, cabin, pilot.getAttribute("pilot_ID"), new Attribute ("pilot_ID", false, true, cabin));
		sc.createRelationship(route, destination, route.getAttribute("route_ID"), new Attribute("route_ID",false, true, destination));

		sc.createPaths(paths, route, Environment.MAX_PATH_DEPTH);

		AttributeGenerator ag = new AttributeGenerator();

		System.out.println("number of paths found: " + paths.size());

		for (Path p : paths) {
			System.out.println("Final path: "+p.toString());
		}
		System.out.println("\n");
		for (Path p : paths) {
			ArrayList<String> queries = ag.generate(p);
			for (int i = 0; i < queries.size(); i++) {
				System.out.println("query: "+ queries.get(i));
			}
			System.out.println("\n");
		}
		
		
		
		
		
		//System.out.println(sc.buildPaths(null, route, 3));
		//System.out.println(sc.createPaths(partial, toReturn, targetTable, max_length);
		//Table featureTable = attributeGenerate (sc, p, "target-attribute");
	}

	public static void printVerbose(String toPrint) {
		if(printMode==Environment.PRINT_MODE_VERBOSE)
			System.out.println(toPrint);
	}

}
