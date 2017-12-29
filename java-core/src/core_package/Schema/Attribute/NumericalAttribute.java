package core_package.Schema.Attribute;

import core_package.Schema.Table;

public class NumericalAttribute extends Attribute {

    public NumericalAttribute(String attributeName, boolean isPrimaryKey, Table t) {
        super(attributeName, isPrimaryKey, t);
    }

    public NumericalAttribute(String attributeName, Table t) {
        super(attributeName, false, t);
    }

    public NumericalAttribute(String attributeName) {
        super(attributeName, false, null);
    }

}
