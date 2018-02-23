package core_package.Schema;

import core_package.Exception.NoSuchTableException;

import java.time.Period;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class Schema {

    private HashSet<Table> tables = new HashSet<>();

    public void addTable(Table t) {
        tables.add(t);
    }

    public void addTables(ArrayList<Table> ts) {
        tables.addAll(ts);
    }

    public Set<Table> getTables() {
        return tables;
    }

    public Table getTableByName(String name) throws NoSuchTableException {
        for(Table t: tables) {
            if(t.getName().equalsIgnoreCase(name)) {
                return t;
            }
        }
        return null;
    }

    public boolean hasTable(String name) {
        for(Table t: tables) {
            if(t.getName().equalsIgnoreCase(name)) {
                return true;
            }
        }
        return false;
    }

    public ArrayList<Relationship> getRelationships() {
        ArrayList<Relationship> rels = new ArrayList<>();
        for(Table t: tables) {
            rels.addAll(t.getRelationships());
        }

        return rels;
    }
}
