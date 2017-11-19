package core_package.Schema.Attribute;

import core_package.Schema.Table;

public class ForeignKeyNumericalAttribute extends NumericalAttribute {

    private Table link;

    public ForeignKeyNumericalAttribute(String attributeName, Table parent, Table link) {
        super(attributeName, parent);
        this.link = link;
    }

    public Table getLink() {
        return link;
    }
}
