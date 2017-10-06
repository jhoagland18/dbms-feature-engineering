package core_package;

public class Relationship {
	
	private Table table;
	private String attributeName;
	
	public Relationship (Table t, String attName) {
		table=t;
		attributeName=attName;	
	}
}
