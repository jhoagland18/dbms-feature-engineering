package core_package.SchemaMapper;
import core_package.Main;
import core_package.Schema.Attribute;
import core_package.Schema.Schema;
import core_package.Schema.Relationship;
import core_package.Schema.Table;
import org.w3c.dom.Attr;

import javax.naming.NameAlreadyBoundException;
import java.util.ArrayList;

public class SchemaBuilder {



    private Schema schema;
    private DatabaseConnection conn;

    public SchemaBuilder() {
        schema = new Schema();
    }

    public SchemaBuilder(String connectionType) {
        this();
        conn = DatabaseConnection.getConnectionForDBType(connectionType);
    }

    public void setConnectionType(String connectionType) {
        conn = DatabaseConnection.getConnectionForDBType(connectionType);
    }

    public Schema getSchema() {
        return schema;
    }

    public SchemaBuilder buildSchema() throws Exception {
        ArrayList<Table> detectedTables = conn.getTables();
        schema.addTables(detectedTables);

        String pkName;



        for(Table t: schema.getTables()) { //identify primary keys

             pkName = conn.getPrimaryKeyNameForTable(t);

            String pkType  = conn.getAttributeType(pkName,t.getName());

           //determine attribute type
        }

        ArrayList<ArrayList<Attribute>> primaryKeys = new ArrayList<ArrayList<Attribute>>();
        for(Table t: schema.getTables()) {
            primaryKeys.add(t.getPrimaryKey());
        }

        for(Table t: schema.getTables()) { //identify foreign keys
            ArrayList<Attribute> newAttributes = conn.getTableAttributes(t.getName());

            /*
            for (int i = 0; i < newAttributes.size(); i++) {
                Attribute att = newAttributes.get(i);


                Attribute collidingPrimaryKey = arrayListsContain(primaryKeys, att.getAttributeName());
                if(collidingPrimaryKey!=null) {
                    if(collidingPrimaryKey!=t.getPrimaryKey()) {
                        String attType = conn.getAttributeType(att.getAttributeName(),t.getTableName());
                        Attribute newForeignKey = null;
                        //detect attribute type and cardinality
                        //create relationship
                    }
                } else if (!(att.ge tAttributeName().equalsIgnoreCase(t.getPrimaryKey().getAttributeName()))) {
                    try {
                        t.addAttribute(newAttributes.get(i));
                    } catch (NameAlreadyBoundException e) {
                        e.printStackTrace();
                    }
                }
            }
            */
        }





        //TODO
        //create attribute objects for each table
        //get foreign keys and add them to their parent tables
        //create relationships based on foreign keys

        return this;
    }
    /*
    private Attribute arrayListsContain(ArrayList<ArrayList<Attribute>> primary, String name) {
        Attribute collision = null;
        for(int i=0; i<primary.size(); i++) {
            if(primary.get(i).getAttributeName().equalsIgnoreCase(name)) {
                collision = primary.get(i);
            }
        }
        return collision;
    }
    */
}
