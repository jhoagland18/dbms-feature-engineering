package core_package.SchemaBuilder;
import core_package.Exception.NoPrimaryKeyIdentifiedException;
import core_package.Exception.NoSuchDatabaseTypeException;
import core_package.Schema.*;

import javax.naming.directory.NoSuchAttributeException;
import java.lang.reflect.Constructor;
import java.sql.ResultSet;
import java.util.*;

public class SchemaBuilder {

    private HashMap<String, ArrayList<Double>> presetBins = new HashMap<String, ArrayList<Double>>();
    private HashMap<String, ArrayList<String>> presetImportantValues = new HashMap<String, ArrayList<String>>();

    //database data types

    //numeric data types
    protected static String DATA_TYPE_INT = "int";
    protected static String DATA_TYPE_BIG_INT = "bigint";
    protected static String DATA_TYPE_BIT_INT = "bit";
    protected static String DATA_TYPE_SMALL_INT = "smallint";
    protected static String DATA_TYPE_TINY_INT = "tinyint";
    protected static String DATA_TYPE_DECIMAL = "decimal";
    protected static String DATA_TYPE_MONEY = "money";
    protected static String DATA_TYPE_NUMERIC = "numeric";
    protected static String DATA_TYPE_FLOAT = "float";
    protected static String DATA_TYPE_REAL = "real";
    protected static final Set<String> DATA_TYPES_NUMERIC = new HashSet<String>(Arrays.asList(
            new String[] {DATA_TYPE_INT, DATA_TYPE_BIG_INT, DATA_TYPE_SMALL_INT, DATA_TYPE_TINY_INT,
            DATA_TYPE_BIT_INT, DATA_TYPE_DECIMAL, DATA_TYPE_MONEY, DATA_TYPE_NUMERIC, DATA_TYPE_FLOAT, DATA_TYPE_REAL}));

    //string data types
    protected static String DATA_TYPE_CHAR = "char";
    protected static String DATA_TYPE_VARCHAR = "varchar";
    protected static String DATA_TYPE_TEXT = "text";
    protected static String DATA_TYPE_NTEXT = "ntext";
    protected static final Set<String> DATA_TYPES_TEXT = new HashSet<String>(Arrays.asList(
            new String[] {DATA_TYPE_CHAR, DATA_TYPE_VARCHAR, DATA_TYPE_TEXT, DATA_TYPE_NTEXT}));

    //time data types
    protected static String DATA_TYPE_TIME = "time";
    protected static String DATA_TYPE_DATETIME = "datetime";
    protected static String DATA_TYPE_DATE = "date";
    protected static final Set<String> DATA_TYPES_TIME = new HashSet<String>(Arrays.asList(
            new String[]{DATA_TYPE_TIME, DATA_TYPE_DATETIME, DATA_TYPE_DATE}));

    private Schema schema;
    private DatabaseConnection conn;

    public SchemaBuilder() {
        schema = new Schema();
    }

    public SchemaBuilder(String connectionType) throws NoSuchDatabaseTypeException {
        this();
        conn = DatabaseConnection.getConnectionForDBType(connectionType);
    }

    public void setConnectionType(String connectionType) throws NoSuchDatabaseTypeException {
        conn = DatabaseConnection.getConnectionForDBType(connectionType);
    }

    public Schema getSchema() {
        return schema;
    }

    public SchemaBuilder buildSchema() throws Exception {

        addTablesToSchema(); //adds tables to schema

        for(Table t: schema.getTables()) { //iterates through tables and adds attributes and primary keys
            addAttributesToTable(t);
        }


        addRelationships(); //identifies relationships between tables and saves to schema

        //TODO
        //create attribute objects for each table
        //get foreign keys and add them to their parent tables
        //create relationships based on foreign keys

        return this;
    }

    private void addAttributesToTable(Table t) throws Exception {
        String pKeyName = getPrimaryKeyNameForTable(t);

        ResultSet rs = conn.query(conn.buildSQLToGetTableAttributeNameAndDatatype(t.getName())); //col 1 is name, col 2 is data type
        while(rs.next()) {
            String attName = rs.getString(1);
            String attDataType = rs.getString(2);
            Attribute newAttribute = createAttribute(attName, attDataType, t);

            if(newAttribute.getAttributeName().equals(pKeyName)) {
                t.setPrimaryKey(newAttribute);
            } else {
                t.addAttribute(newAttribute);
            }
        }
    }

    public Attribute createAttribute(String attName, String attDataType, Table t) throws Exception {

            Class<?> attributeClass;
            Attribute newAttribute = null;
            if(attName.contains("ID")) {
                attributeClass = Class.forName(IDAttribute.class.getName());
                Class[] params = new Class[] {String.class};
                Constructor cons = attributeClass.getConstructor(params);
                newAttribute = (Attribute)cons.newInstance(attName);
                return newAttribute;

            } else if(DATA_TYPES_NUMERIC.contains(attDataType)) {

                if(attDataType.contains("int") || attDataType.contains("bit")) { //checks for zero/one attribute, otherwise create a regular numeric attribute
                    ResultSet rs = conn.query(conn.buildSQLToGetUniqueRows(attName, t));
                    boolean oneorzero = true;
                    int i=1;
                    while(rs.next() && oneorzero) {
                        if(i>2 || !( rs.getString(1).equals("0") || rs.getString(1).equals("1") )) {
                            oneorzero = false;
                        }
                        i++;
                    }

                    if(oneorzero) {
                        attributeClass = Class.forName(ZeroOneAttribute.class.getName());
                        Class[] params = new Class[] {String.class};
                        Constructor cons = attributeClass.getConstructor(params);

                        newAttribute = (Attribute)cons.newInstance(attName);
                        return newAttribute;
                    }
                }

                attributeClass = Class.forName(NumericAttribute.class.getName());
                Class[] params = new Class[] {String.class, String.class, ArrayList.class};
                Constructor cons = attributeClass.getConstructor(params);

                ArrayList<Double> binThreshholds = getBins(attName, t);

                newAttribute = (Attribute)cons.newInstance(attName, "", binThreshholds);
                return newAttribute;

            } else if(DATA_TYPES_TEXT.contains(attDataType)) {
                attributeClass = Class.forName(NominalAttribute.class.getName());
                Class[] params = new Class[]{String.class, String.class, ArrayList.class};
                Constructor cons = attributeClass.getConstructor(params);

                ArrayList<String> importantValues = getImportantValues(attName, t);

                newAttribute = (Attribute) cons.newInstance(attName, "", importantValues);
                return newAttribute;

            } else if(DATA_TYPES_TIME.contains(attDataType)) { //TODO finish datetime case
                attributeClass = Class.forName(NominalAttribute.class.getName());
                Class[] params = new Class[]{String.class, String.class, ArrayList.class};
                Constructor cons = attributeClass.getConstructor(params);

                ArrayList<String> importantValues = getImportantValues(attName, t);

                newAttribute = (Attribute) cons.newInstance(attName, "", importantValues);
                return newAttribute;
            }
        throw new NoSuchAttributeException("Cannot determine attribute type from input ("+attName+", "+attDataType+")");
    }

    /**
     * returns user defined bins, or defines its own bins based on distribution in the database
     * @param attName
     * @param t
     * @return
     */
    private ArrayList<Double> getBins(String attName, Table t) {
        if(presetBins.containsKey(attName)) {
            return presetBins.get(attName);
        } else { //TODO: automatic bin detection
            return null;
        }
    }

    /**
     * returns user defined important values, or defines its own important values based on distribution in the database
     * @param attName
     * @param t
     * @return
     */
    private ArrayList<String> getImportantValues(String attName, Table t) {
        if(presetImportantValues.containsKey(attName)) {
            return presetImportantValues.get(attName);
        } else { //TODO: automatic important value detection
            return null;
        }
    }


    /**
     * @throws Exception
     */
    private void addTablesToSchema() throws Exception {
        ArrayList<Table> detectedTables = new ArrayList<Table>();
        ResultSet rs = conn.query(conn.buildSQLToRetrieveTables());

        while (rs.next()) {
            schema.addTable(new Table(rs.getString(1)));
        }
    }

    /**
     * returns name of primary key for defined table
     * if primary key constraint is not set, primary key is chosen by the column name most lexiographically similar to the table name and is unique among all rows
     * @param t
     * @return
     * @throws Exception
     */
    private String getPrimaryKeyNameForTable(Table t) throws Exception {
        String attName="";
            ResultSet rs = conn.query(conn.buildSQLToRetrievePrimaryKeyFromTable(t.getName()));
            if(rs.next()) { //if primary key constraint is set
                attName = rs.getString(1);
            } else { //primary key constraint not set
                //Use attribute that is lexiographically most similar to table name

                int mostSimilar = Integer.MAX_VALUE;
                rs = conn.query(conn.buildSQLToGetTableAttributeNameAndDatatype(t.getName()));
                if(rs.next()) {
                    attName = rs.getString(1);
                    mostSimilar = Math.abs(rs.getString(1).compareTo(t.getName()));

                    String attDataType;
                    int charDiff;

                    do {
                        charDiff = rs.getString(1).compareTo(t.getName());
                        attDataType = rs.getString(2);
                        if (Math.abs(charDiff) < mostSimilar && DATA_TYPES_NUMERIC.contains(attDataType) && isUnique(attName, t)) {
                            mostSimilar = Math.abs(charDiff);
                            attName = rs.getString(1);

                        }
                    } while (rs.next());
                } else { //no attributes in table
                    return "";
                }
            }
        if(attName.isEmpty()) {
                throw new NoPrimaryKeyIdentifiedException("No primary key can be identified for table " + t.getName());
        } else {
                return attName;
        }
    }

    public boolean isUnique(String attName, Table t) throws Exception {
        ResultSet rs = conn.query(conn.buildSQLToGetDifferenceBetweenTotalAndNumUnique(attName, t));
        int diff = Integer.parseInt(rs.getString(1));
        return diff==0 ? true : false;
    }

    /**
     * @throws Exception
     */
    private void addRelationships() throws Exception {
        for (Table t1: schema.getTables()) { //identify foreign keys
            String pkName = t1.getPrimaryKey().get(0).getAttributeName();

            for(Table t2: schema.getTables()) {
                if(t1!=t2) {
                    ArrayList<Attribute> t2Attributes = t2.getAttributesByType(IDAttribute.class);
                    for(Attribute a: t2Attributes) {
                        if(a instanceof IDAttribute && pkName.equals(a.getAttributeName())) { //relationship based on attribute name established
                            RelationshipType rt = RelationshipType.ToN;
                            ResultSet rs = conn.query(conn.buildSQLToGetDifferenceBetweenTotalAndNumUnique(a.getAttributeName(),t2));
                            if(rs.next()) {
                                if(rs.getString(1).equals("0"))
                                    rt = RelationshipType.To1;
                            }
                            Relationship rel = new Relationship(t1,t2,(IDAttribute)t1.getPrimaryKey().get(0), (IDAttribute)a, rt);
                            schema.addRelationship(rel);
                        }
                    }


                }
            }
        }
    }
}
