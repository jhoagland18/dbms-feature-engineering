package core_package.Schema;

import core_package.Schema.Attribute.Attribute;

public class ForeignKey extends Attribute {
	private Table link;
	public ForeignKey(String attributeName, Table parent, Table link) {
		super(attributeName, false, parent);
		// TODO Auto-generated constructor stub
		this.link = link;
	}
	

	@Override
	public String fillTemplate(String[] accessPoints, Relationship rel) {
		// TODO Auto-generated method stub
		return null;
	}

	public Table getLink() {
        return link;
    }
}
