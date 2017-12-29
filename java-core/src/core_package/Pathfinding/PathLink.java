package core_package.Pathfinding;

import core_package.Schema.Relationship;
import core_package.Schema.Table;



public class PathLink {

    private final Relationship rel;
    private final Table lastTable;

    public PathLink(Relationship rel, Table lastTable) {
        this.rel = rel;
        this.lastTable = lastTable;
    }

    public Relationship getRelationship() {
        return rel;
    }

    public Table getLastTable() {
        return lastTable;
    }

    public String toString() {
        return rel.toString();
    }
}
