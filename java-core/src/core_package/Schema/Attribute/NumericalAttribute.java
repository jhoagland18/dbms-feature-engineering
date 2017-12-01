package core_package.Schema.Attribute;

import core_package.Schema.Relationship;
import core_package.Schema.Table;

public class NumericalAttribute extends Attribute {

    public NumericalAttribute(String attributeName, boolean isPrimaryKey, Table t) {
        super(attributeName, isPrimaryKey, t);
    }

    public NumericalAttribute(String attributeName, Table t) {
        super(attributeName, false, t);
    }

	@Override
	public String fillTemplate(String[] accessPoints, Relationship rel) {
		Table b = null;
		if (rel.getTables()[0] == this.getParentTable()) {
			b = rel.getTables()[1];
		}
		else if (rel.getTables()[1] == this.getParentTable()) {
			b = rel.getTables()[0];
		}
		else {
			throw new Exception("Relationship does not contain table with specified attribute");
		}
		
		StringBuilder template = new StringBuilder ("select " + this.getParentTable().getTableName() + "." + this.getParentTable().getPrimaryKey());
		for (int i = 0; i < Attribute.SQL_NUMERICAL_FUNCTIONS.length; i++) {
			template.append(Attribute.SQL_NUMERICAL_FUNCTIONS[i] + ", (" +this.getAttributeName() + ")");
		}
		template.append("\nfrom " + this.getParentTable().getTableName());
		template.append("\n left outer join " + b.getTableName());
		template.append("\n on " + this.getParentTable().getTableName() + "." + this.getParentTable().getPrimaryKey() + " = " + b.getTableName()
				+ "." + this.getParentTable().getPrimaryKey());
		// TODO Auto-generated method stub
	
		return template.toString();
	}

    
    
}
