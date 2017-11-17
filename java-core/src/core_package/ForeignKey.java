package core_package;

public class ForeignKey extends Attribute {

	private Table linkedTable;
	public ForeignKey(String attributeName, boolean isPKey, Table t, Table linked) {
		super(attributeName, isPKey, t);
		linkedTable = linked;
		// TODO Auto-generated constructor stub
	}

}
