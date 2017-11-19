package core_package.Schema.Attribute;

import core_package.Schema.Table;

public class ForeignKeyCategoricalAttribute extends CategoricalAttribute {

    private Table link;

    public ForeignKeyCategoricalAttribute(String attributeName, Table parent, Table link) {
        super(attributeName, parent);
        this.link = link;
    }

    public Table getLink() {
        return link;
    }
}
