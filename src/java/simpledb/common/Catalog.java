package simpledb.common;

import simpledb.storage.DbFile;
import simpledb.storage.HeapFile;
import simpledb.storage.TupleDesc;

import java.io.*;
import java.util.*;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 * 
 * @Threadsafe
 */
public class Catalog {

    class TableIdIterator implements Iterator<Integer> {
        private int index;

        public TableIdIterator() {
            this.index = 0;
        }

        @Override
        public boolean hasNext() {
            if (index < tupleDescs.size())
                return true;
            else
                return false;
        }

        @Override
        public Integer next() {
            index++;
            return dbFiles.get(this.index).getId();
        }
    }

    // 类域，存储了一组tupleDesc，每个tupleDesc与DbFiles中的一个元素对应
    private ArrayList<TupleDesc> tupleDescs = new ArrayList<>();
    private ArrayList<DbFile> dbFiles = new ArrayList<>();
    private ArrayList<String> names = new ArrayList<>();
    private ArrayList<String> primaryKeys = new ArrayList<>();
    private ArrayList<Integer> tableIds = new ArrayList<>();

    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    public Catalog() {
        // some code goes here
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     * 
     * @param file      the contents of the table to add; file.getId() is the
     *                  identfier of
     *                  this file/tupledesc param for the calls getTupleDesc and
     *                  getFile
     * @param name      the name of the table -- may be an empty string. May not be
     *                  null. If a name
     *                  conflict exists, use the last table to be added as the table
     *                  for a given name.
     * @param pkeyField the name of the primary key field
     */
    public void addTable(DbFile file, String name, String pkeyField) {
        // some code goes here
        boolean isAdded = false;
        for (int i = 0; i < this.dbFiles.size(); ++i) {
            int id = file.getId();
            if (Objects.equals(name, this.names.get(i)) || id == this.dbFiles.get(i).getId()) {
                // 更新第i项的数据
                this.dbFiles.set(i, file);
                this.names.set(i, name);
                this.primaryKeys.set(i, pkeyField);
                this.tupleDescs.set(i, file.getTupleDesc());
                this.tableIds.set(i, file.getId());
                isAdded = true;
            }
        }
        if (isAdded == false) {
            // 要考虑name和fileId的冲突，若有冲突则将相关数据更新
            this.tupleDescs.add(file.getTupleDesc());
            this.primaryKeys.add(pkeyField);
            this.names.add(name);
            this.dbFiles.add(file);
            this.tableIds.add(file.getId());
        }
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     * 
     * @param file the contents of the table to add; file.getId() is the identfier
     *             of
     *             this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     * 
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        // some code goes here
        for (int i = 0; i < this.dbFiles.size(); ++i) {
            if (Objects.equals(name, this.names.get(i)))
                return this.dbFiles.get(i).getId();
        }
        throw new NoSuchElementException();
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * 
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *                function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        // some code goes here
        for (int i = 0; i < this.dbFiles.size(); ++i) {
            if (this.tableIds.get(i) == tableid)
                return this.tupleDescs.get(i);
        }
        throw new NoSuchElementException();
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     * 
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *                function passed to addTable
     */
    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
        for (int i = 0; i < this.dbFiles.size(); ++i) {
            if (this.tableIds.get(i) == tableid)
                return this.dbFiles.get(i);
        }
        throw new NoSuchElementException();
    }

    public String getPrimaryKey(int tableid) {
        // some code goes here
        for (int i = 0; i < this.dbFiles.size(); ++i) {
            if (this.dbFiles.get(i).getId() == tableid)
                return this.primaryKeys.get(i);
        }
        throw new NoSuchElementException();
    }

    public Iterator<Integer> tableIdIterator() {
        // some code goes here
        return new TableIdIterator();
    }

    public String getTableName(int id) {
        // some code goes here
        for (int i = 0; i < this.dbFiles.size(); ++i) {
            if (this.dbFiles.get(i).getId() == id)
                return this.names.get(i);
        }
        throw new NoSuchElementException();
    }

    /** Delete all tables from the catalog */
    public void clear() {
        // some code goes here
        this.primaryKeys = new ArrayList<>();
        this.dbFiles = new ArrayList<>();
        this.tupleDescs = new ArrayList<>();
        this.names = new ArrayList<>();
    }

    public void resetTableId(int oldId, int newId) {
        for (int i = 0; i < this.tableIds.size(); ++i) {
            if (this.tableIds.get(i) == oldId)
                this.tableIds.set(i, newId);
        }
    }

    /**
     * Reads the schema from a file and creates the appropriate tables in the
     * database.
     * 
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder = new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(catalogFile));

            while ((line = br.readLine()) != null) {
                // assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                // System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<>();
                ArrayList<Type> types = new ArrayList<>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().equalsIgnoreCase("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().equalsIgnoreCase("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder + "/" + name + ".dat"), t);
                addTable(tabHf, name, primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}
