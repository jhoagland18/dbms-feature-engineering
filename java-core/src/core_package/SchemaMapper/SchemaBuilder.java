package core_package.SchemaMapper;
import core_package.Main;
import core_package.Schema.Attribute.Attribute;
import core_package.Schema.DBSchema;
import core_package.Schema.Relationship;
import core_package.Schema.Table;
import org.w3c.dom.Attr;

import javax.naming.NameAlreadyBoundException;
import java.util.ArrayList;

public class SchemaBuilder {



    private DBSchema schema;
    private DatabaseConnection conn;

    public SchemaBuilder() {
        schema = new DBSchema();
    }

    public SchemaBuilder(String connectionType) {
        this();
        conn = DatabaseConnection.getConnectionForDBType(connectionType);
    }

    public void setConnectionType(String connectionType) {
        conn = DatabaseConnection.getConnectionForDBType(connectionType);
    }

    public DBSchema getSchema() {
        return schema;
    }

    public SchemaBuilder buildSchema() throws Exception {
        ArrayList<Table> detectedTables = conn.getTables();
        Main.printVerbose("tables: "+detectedTables.toString());
        schema.addTables(detectedTables);

        String pkName;

        for(Table t: schema.getTables()) { //identify primary keys

             pkName = conn.getPrimaryKeyNameForTable(t);

            String pkType  = conn.getAttributeType(pkName,t.getTableName());

            if(pkType == DatabaseConnection.ATTRIBUTE_TYPE_CATEGORICAL ) {
                t.addCatagoricalAttribute(pkName,true);
            }
            else if(pkType == DatabaseConnection.ATTRIBUTE_TYPE_DATE ) {
                t.addDateAttribute(pkName,true);
            }
            else if(pkType == DatabaseConnection.ATTRIBUTE_TYPE_NUMERICAL ) {
                t.addNumericalAttribute(pkName,true);
            }
        }

        ArrayList<Attribute> primaryKeys = new ArrayList<Attribute>();
        for(Table t: schema.getTables()) {
            primaryKeys.add(t.getPrimaryKey());
        }

        for(Table t: schema.getTables()) { //identify foreign keys
            ArrayList<Attribute> newAttributes = conn.getTableAttributes(t.getTableName());

            for (int i = 0; i < newAttributes.size(); i++) {
                Attribute att = newAttributes.get(i);

                Attribute collidingPrimaryKey = arrayListsContain(primaryKeys, att.getAttributeName());
                if(collidingPrimaryKey!=null) {
                    if(collidingPrimaryKey!=t.getPrimaryKey()) {
                        String attType = conn.getAttributeType(att.getAttributeName(),t.getTableName());
                        Attribute newForeignKey = null;
                        if(attType == DatabaseConnection.ATTRIBUTE_TYPE_CATEGORICAL ) {
                            newForeignKey = t.addCategoricalForeignKey(att.getAttributeName(), collidingPrimaryKey.getParentTable());
                        }
                        else if(attType == DatabaseConnection.ATTRIBUTE_TYPE_DATE ) {
                            newForeignKey = t.addDateForignKey(att.getAttributeName(),collidingPrimaryKey.getParentTable());
                        }
                        else if(attType == DatabaseConnection.ATTRIBUTE_TYPE_NUMERICAL ) {
                            newForeignKey = t.addNumericalForeignKey(att.getAttributeName(),collidingPrimaryKey.getParentTable());
                        }
                        schema.createRelationship(collidingPrimaryKey, newForeignKey, Relationship.ONE_TO_ONE);
                    }
                } else if (!(att.getAttributeName().equalsIgnoreCase(t.getPrimaryKey().getAttributeName()))) {
                    Main.printVerbose("adding attribute " + newAttributes.get(i).getAttributeName() + " to " + t.getTableName());
                    try {
                        t.addAttribute(newAttributes.get(i));
                    } catch (NameAlreadyBoundException e) {
                        e.printStackTrace();
                    }
                }
            }
        }





        //TODO
        //create attribute objects for each table
        //get foreign keys and add them to their parent tables
        //create relationships based on foreign keys

        return this;
    }

    private Attribute arrayListsContain(ArrayList<Attribute> primary, String name) {
        Attribute collision = null;
        for(int i=0; i<primary.size(); i++) {
            if(primary.get(i).getAttributeName().equalsIgnoreCase(name)) {
                collision = primary.get(i);
            }
        }
        return collision;
    }
}
