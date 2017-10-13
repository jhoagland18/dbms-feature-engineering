package core_package;

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
		sc.createRelationship(route,  flight,  new Attribute("route_ID", true, route), new Attribute("route_ID", false, flight));
		sc.buildPaths(null, route, 3);
		//Table featureTable = attributeGenerate (sc, p, "target-attribute");
		sc.printPaths();
	}

}
