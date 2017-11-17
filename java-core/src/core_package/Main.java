package core_package;

import java.util.ArrayList;

import static java.lang.Thread.yield;

//Created by Gayatri Krishnan and Jackson Hoagland on 9/29/2017

public class Main {

	static ArrayList<Path> paths;
	static final boolean verbose=true; //enable for verbose output logging

	public static
	void main(String args[]) {
		
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
		
		
		//route.addRelationship(flight,"flight_ID");
		DBSchema sc = new DBSchema();
		sc.addTable(flight);
		sc.addTable(route);
		sc.addTable(pilot);
		sc.addTable(cabin);
		sc.addTable(destination);
		
		sc.createRelationship(route, flight, route.getAttribute("route_ID"), new Attribute("route_ID", false, true, flight));
		sc.createRelationship(flight, pilot, flight.getAttribute("flight_ID"), new Attribute ("flight_ID", false, true, pilot));
		sc.createRelationship(pilot, cabin, pilot.getAttribute("pilot_ID"), new Attribute ("pilot_ID", false, true, cabin));
		sc.createRelationship(route, destination, route.getAttribute("route_ID"), new Attribute("route_ID",false, true, destination));
		
		paths = new ArrayList<Path>();
		sc.createPaths(paths, route, 3);
		AttributeGenerator ag = new AttributeGenerator();

		System.out.println("number of paths found: " + paths.size());

		for (Path p : paths) {
			ArrayList<String> queries = ag.generate(p);
			System.out.println("Final path: "+p.toString());
			for (int i = 0; i < queries.size(); i++) {
				System.out.println("query: "+ queries.get(i));
			}
		}
		
		
		
		
		
		//System.out.println(sc.buildPaths(null, route, 3));
		//System.out.println(sc.createPaths(partial, toReturn, targetTable, max_length);
		//Table featureTable = attributeGenerate (sc, p, "target-attribute");
	}

	public static void printVerbose(String toPrint) {
		if(Main.verbose)
			System.out.println(toPrint);
	}

}
