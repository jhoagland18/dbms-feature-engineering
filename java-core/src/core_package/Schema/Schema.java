package core_package.Schema;

import java.time.Period;
import java.util.ArrayList;

public class Schema {

    private ArrayList<Table> tables = new ArrayList<>();
    private ArrayList<Relationship> relationships = new ArrayList<>();


    public void addTable(Table t) {
        tables.add(t);
    }

    public void addTables(ArrayList<Table> ts) {
        tables.addAll(ts);
    }

    public ArrayList<Table> getTables() {
        return tables;
    }

    public ArrayList<Relationship> getRelationships() {
        return relationships;
    }

    public String toString() {
        String toReturn = "Schema:";
        for(Table t: tables) {
            toReturn+=t.getName()+":\nAttributes:\n"+t.getPrimaryKey().get(0).getAttributeName()+" ("+t.getPrimaryKey().get(0).getClass().getSimpleName()+") (PK)\n";
            for(Attribute a: t.getAttributes()) {
                toReturn+=a.getAttributeName()+" ("+a.getClass().getSimpleName()+")\n";
            }
            toReturn+="\n";
        }
        toReturn+="Relationships:\n";
        for(Relationship rel: relationships) {
            toReturn+=rel.getTable1().getName() + "("+rel.getAttributes1().get(0).getAttributeName()+") -> " + rel.getTable2().getName() + "(" + rel.getAttributes2().get(0).getAttributeName() + ") "+rel.getRelationshipType()+"\n";
        }
        return toReturn;
    }

    public void addRelationship(Relationship rel) {
        relationships.add(rel);
    }
}
