package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
	private int num_fields;
	private TDItem[] TDArray;

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

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }

    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return new ItemIterator();
        // done
    }
    
    private class ItemIterator implements Iterator<TDItem>{
    	private int current=0;
    	public boolean hasNext() {
    		return TDArray.length>current;
    	}
    	public TDItem next() {
    		if (!hasNext()) throw new NoSuchElementException();
    		current++;
    		return TDArray[current];
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
        // some code goes here
    	this.num_fields=typeAr.length;
    	if (this.num_fields==0) throw new IllegalArgumentException("It is a null array");
    	if (typeAr.length!=fieldAr.length) throw new IllegalArgumentException("Length of type aray not consistent with field array");
    	this.TDArray=new TDItem[this.num_fields];
    	for (int i=0;i<this.num_fields;i++) {
    		TDArray[i]=new TDItem(typeAr[i],fieldAr[i]);
    	}
    	//done
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
        // some code goes here
    	this(typeAr, new String[typeAr.length]);
    	// done
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return this.num_fields;
        // done
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
        // some code goes here
    	if (i>=this.num_fields||i<0) throw new NoSuchElementException("i is not a valid field reference");
        return TDArray[i].fieldName;
        // done
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
        // some code goes here
    	if (i>=this.num_fields||i<0) throw new NoSuchElementException("i is not a valid field reference");
        return TDArray[i].fieldType;
        // done
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
        // some code goes here
    	for (int i=0;i<this.num_fields;i++) {
    		if (TDArray[i].fieldName!=null && TDArray[i].fieldName.equals(name)) return i;
    	}
        throw new NoSuchElementException("No field with a matching name is found");
        // done
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
    	int temp=0;
    	for (int i=0;i<this.num_fields;i++) {
    		temp+=TDArray[i].fieldType.getLen();
    	}
        return temp;
        // done
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
        // some code goes here
    	int sum_fields=td1.numFields()+td2.numFields();
    	TDItem[] td1_copy=td1.TDArray;
    	TDItem[] td2_copy=td2.TDArray;
    	Type[] merge_type=new Type[sum_fields];
    	String[] merge_string=new String[sum_fields];
    	for (int i=0;i<sum_fields;i++) {
    		if (i<td1.numFields()) {
    			merge_type[i]=td1_copy[i].fieldType;
    			merge_string[i]=td1_copy[i].fieldName;
    		}else {
    			merge_type[i]=td2_copy[i-td1.numFields()].fieldType;
    			merge_string[i]=td2_copy[i-td1.numFields()].fieldName;
    		}
    	}
        return new TupleDesc(merge_type,merge_string);
        // done
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
        // some code goes here
    	if (this==o) return true;
    	if (o instanceof TupleDesc) {
    		boolean size_eq=((TupleDesc) o).getSize()==this.getSize()&&((TupleDesc) o).numFields()==this.num_fields;
    		if (size_eq==true) {
    			for (int i=0;i<this.num_fields;i++) {
    				if (this.TDArray[i].fieldType==((TupleDesc) o).getFieldType(i)&&this.TDArray[i].fieldName==((TupleDesc) o).getFieldName(i));
    				else return false;
    			}
    			return true;
    		}else return false;
    	}else
        return false;
    	// done
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
        // some code goes here
    	StringBuffer out=new StringBuffer();
    	for (TDItem tdinstance:this.TDArray) {
    		out.append(tdinstance.toString()+",");
    	}
    	String output=out.toString();
        return output;
        // done
    }
}
