package core_package;

import java.util.ArrayList;

//Created by Gayatri Krishnan and Jackson Hoagland on 9/29/2017

public class Main {
	
	public static void main(String args[]) {
		
		Table route = new Table("route"); 
		route.addAttribute("route_ID", true);
		Table flight = new Table ("flight");
		flight.addAttribute("flight_ID", true);
		
		
		//route.addRelationship(flight,"flight_ID");
		DBSchema sc = new DBSchema();
		sc.addTable(flight);
		sc.addTable(route);
		sc.createRelationship(route, flight, route.getAttribute("route_ID"), new Attribute("route_ID", false, flight));
		//sc.buildPaths(null, route, 3);
		ArrayList<Relationship> routeRels = flight.getRelationships();
		for (Relationship r : routeRels ) {
			System.out.println("Route relationships: "+r.getTables()[0]+", "+r.getTables()[1]);
		}
		System.out.println(sc.buildPaths(null, route, 3));
		//Table featureTable = attributeGenerate (sc, p, "target-attribute");
		//sc.printPaths();
	}

}
