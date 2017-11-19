package core_package.Schema.Attribute;

import core_package.Schema.Table;

public class ForeignKeyDateAttribute extends DateAttribute {

    private Table link;

    public ForeignKeyDateAttribute(String attributeName, Table parent, Table link) {
        super(attributeName, parent);
        this.link = link;
    }

    public Table getLink() {
        return link;
    }
}
