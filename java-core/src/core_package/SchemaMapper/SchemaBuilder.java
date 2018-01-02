package core_package.SchemaMapper;
import core_package.Schema.Attribute;
import core_package.Schema.Schema;
import core_package.Schema.Table;
import java.sql.ResultSet;
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

        addTables(); //adds tables to schema

        addPrimaryKeysToTables(schema.getTables()); //adds primary keys to tables

        addAttributesToTables(schema.getTables()); //creates attributes, tries to determine type, adds to table

        addRelationships(); //identifies relationships between tables and saves to schema





        //TODO
        //create attribute objects for each table
        //get foreign keys and add them to their parent tables
        //create relationships based on foreign keys

        return this;
    }


    /**
     * @throws Exception
     */
    private void addTables() throws Exception {
        ArrayList<Table> detectedTables = new ArrayList<Table>();
        ResultSet rs = conn.query(conn.buildSQLToRetrieveTables());

        while (rs.next()) {
            schema.addTable(new Table(rs.getString(1)));
        }
    }

    /**
     * @param tables
     * @throws Exception
     */
    private void addPrimaryKeysToTables(ArrayList<Table> tables) throws Exception {
        String pkName="";

        for(Table t: tables) { //identify primary keys

            pkName = getPrimaryKeyNameForTable(t);

            String pkType  = getAttributeType(pkName,t.getName());

            //determine attribute type
        }
    }

    /**
     * @param tables
     * @throws Exception
     */
    private void addAttributesToTables(ArrayList<Table> tables) throws Exception {
        ArrayList<ArrayList<Attribute>> primaryKeys = new ArrayList<ArrayList<Attribute>>();
        for (Table t : tables) {
            primaryKeys.add(t.getPrimaryKey());
        }

        for (Table t : schema.getTables()) { //identify foreign keys
            ArrayList<Attribute> newAttributes = getTableAttributes(t.getName());

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
    }

    /**
     * @param tableName
     * @return
     * @throws Exception
     */
    private ArrayList<Attribute> getTableAttributes(String tableName) throws Exception {


            ResultSet rs = conn.query(conn.buildSQLToGetTableAttributes(tableName));

            String attName="";
            String attType="";
            ArrayList<Attribute> newAttributes = new ArrayList<Attribute>();

            int i=0;

            while(rs.next()) {
                attName = rs.getString(1);
                attType = getAttributeType(attName, tableName);

                //determine attribute type and add to newAttributes
            }

            return newAttributes;
        }

    /**
     * @param attributeName
     * @param tableName
     * @return
     * @throws Exception
     */
    private String getAttributeType(String attributeName, String tableName) throws Exception {

        ResultSet rs = conn.query(conn.buildSQLToGetAttributeDataType(attributeName, tableName));
        String attType = "";
        if(rs.next()) {
            attType = rs.getString(1);

        } else {
            throw new Exception("No datatype returned");
        }
        if(attType.equals(DatabaseConnection.DATA_TYPE_INT) || attType.equals(DatabaseConnection.DATA_TYPE_DOUBLE))
        {
            return DatabaseConnection.ATTRIBUTE_TYPE_NUMERICAL;
        } else if(attType.equals(DatabaseConnection.DATA_TYPE_VARCHAR)) {
            return DatabaseConnection.ATTRIBUTE_TYPE_CATEGORICAL;
        } else if(attType.equals(DatabaseConnection.DATA_TYPE_DATETIME)) {
            return DatabaseConnection.ATTRIBUTE_TYPE_DATE;
        } else {
            throw new Exception("No such datatype");
        }
    }

    /**
     * @param t
     * @return
     * @throws Exception
     */
    private String getPrimaryKeyNameForTable(Table t) throws Exception {
        String attName="";
            ResultSet rs = conn.query(conn.buildSQLToRetrievePrimaryKeyFromTable(t.getName()));
            if(rs.next()) {
                attName = rs.getString(1);
            }

        return attName;
    }

    /**
     * @throws Exception
     */
    private void addRelationships() throws Exception {
        for (Table t : schema.getTables()) { //identify foreign keys
            ArrayList<Attribute> newAttributes = getTableAttributes(t.getName());

            /*
            for (int i = 0; i < newAttributes.size(); i++) {
                Attribute att = newAttributes.get(i);


                Attribute collidingPrimaryKey = arrayListsContain(primaryKeys, att.getAttributeName());
                if(collidingPrimaryKey!=null) {
                    if(collidingPrimaryKey!=t.getPrimaryKey()) {
                        String attType = conn.getAttributeDataType(att.getAttributeName(),t.getTableName());
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
}
