package core_package.Schema.Attribute;

import core_package.Schema.Table;

import java.util.ArrayList;

public class CategoricalAttribute extends Attribute {

    ArrayList<String> types;

    public CategoricalAttribute(String attributeName, boolean isPrimaryKey, Table t) {
    super(attributeName, isPrimaryKey, t);

    types = new ArrayList<String>();
    }

    public CategoricalAttribute(String attributeName, Table t) {
        super(attributeName, false, t);
    }

    CategoricalAttribute(String attributeName, boolean isPrimaryKey, Table t, ArrayList<String> types) {
        super(attributeName, isPrimaryKey, t);
        this.types = new ArrayList<String>(types);
    }

    CategoricalAttribute(String attributeName, Table t, ArrayList<String> types) {
        super(attributeName, false, t);
        this.types = new ArrayList<String>(types);
    }

    public void addType(String s) {
        types.add(s);
    }

    public boolean removeType(String s) {
        return types.remove(s);
    }

}
