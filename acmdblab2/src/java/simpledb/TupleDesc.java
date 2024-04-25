package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
    private int numFields;
    private TDItem[] tdAr;

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        
        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        @Override
        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o instanceof TDItem) {
                TDItem another = (TDItem) o;
                //因为fieldName可能为null,所以都为null时视为name相同
                boolean nameEquals = (fieldName == null && another.fieldName == null)
                        || fieldName.equals(another.fieldName);
                boolean typeEquals = fieldType.equals(another.fieldType);
                return nameEquals && typeEquals;
            } else return false;
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return new TDIterator();
    }

    private class TDItemIterator implements Iterator<TDItem> {

        private int pos = 0;

        @Override
        public boolean hasNext() {
            return tdAr.length > pos;
        }

        @Override
        public TDItem next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return tdAr[pos++];
        }
    }


    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        if (typeAr.length == 0) {
            throw new IllegalArgumentException("类型数组至少包含一个元素");
        }
        if (typeAr.length != fieldAr.length) {
            throw new IllegalArgumentException("数组fieldAr长度必须和typeAr一致");
        }

        numFields = typeAr.length;
        tdAr = new TDItem[numFields];

        for (int i = 0; i < numFields; i++) {
            tdAr[i] = new TDItem(typeAr[i], fieldAr[i]);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        this(typeAr, new String[typeAr.length]);
    }

    private TupleDesc(TDItem[] tdItems) {
        if (tdItems == null || tdItems.length == 0) {
            throw new IllegalArgumentException("tdItem数组必须非空且至少包含一个元素");
        }
        this.tdAr = tdItems;
        this.numFields = tdItems.length;
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return this.numFields;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if (i < 0 || i >= numFields) {
            throw new NoSuchElementException();
        }
        return tdAr[i].fieldName;
        
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {

        if (i < 0 || i >= numFields) {
            throw new NoSuchElementException();
        }
        return tdAr[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        if (name == null) {
            throw new NoSuchElementException();
        }
        String fieldName;
        // TODO: 17-5-22 improve this,不要使用遍历
        for (int i = 0; i < tdAr.length; i++) {
            if ((fieldName = tdAr[i].fieldName) != null && fieldName.equals(name)) {
                return i;
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int totalSize = 0;
        for (TDItem item : tdAr) {
            totalSize += item.fieldType.getLen();
        }
        return totalSize;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        int newTdNumFields = td1.numFields() + td2.numFields();
    	Type[] newTdTypeAr = new Type[newTdNumFields];
    	
    	// concatenate both type arrays
    	for(int x=0; x<td1.numFields(); x++)
    		newTdTypeAr[x] = td1.typeAr[x];
    	int startIndexTd2 = td1.numFields();
    	for(int x=startIndexTd2; x<newTdNumFields; x++)
    		newTdTypeAr[x] = td2.typeAr[x-startIndexTd2];
    	
    	// dont specify field name array if neither td has a specified field name array
    	if(td1.fieldAr == null && td2.fieldAr == null)
    		return new TupleDesc(newTdTypeAr);
    	
    	String[] newTdFieldAr = new String[newTdNumFields];
    	// concatenate both field name arrays
    	for(int x=0; x<td1.numFields(); x++)
    		newTdFieldAr[x] = td1.fieldAr == null ? null : td1.fieldAr[x];
    	for(int x=startIndexTd2; x<newTdNumFields; x++)
    		newTdFieldAr[x] = td2.fieldAr == null ? null : td2.fieldAr[x-startIndexTd2];
    	
     	return new TupleDesc(newTdTypeAr, newTdFieldAr);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        if (o==null || !(o instanceof TupleDesc))
    		return false;
    	TupleDesc other_td = (TupleDesc) o;
    	if(other_td.getSize() != this.getSize())
    		return false;
    	for(int x=0; x<typeAr.length; x++) {
    		if(typeAr[x] != other_td.typeAr[x])
    			return false;
    	}
    	return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        StringBuffer result = new StringBuffer();
        result.append("Fields: ");
        for (TDItem tdItem : tdAr) {
            result.append(tdItem.toString() + ", ");
        }
        result.append(numFields + " Fields in all");
        return result.toString();
    }
}
