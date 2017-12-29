package core_package.Schema.Attribute;

import core_package.Schema.Table;

public class DateAttribute extends Attribute {

    public DateAttribute(String attributeName, boolean isPrimaryKey, Table t) {
        super(attributeName, isPrimaryKey, t);
    }

    public DateAttribute(String attributeName, Table t) {
        super(attributeName, false, t);
    }

    public DateAttribute(String attributeName) {
        super(attributeName, false, null);
    }
}
