package hw1;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc {

	private Type[] types;
	// field name
	private String[] fields;

	/**
	 * Create a new TupleDesc with typeAr.length fields with fields of the
	 * specified types, with associated named fields.
	 *
	 * @param typeAr array specifying the number of and types of fields in
	 *        this TupleDesc. It must contain at least one entry.
	 * @param fieldAr array specifying the names of the fields. Note that names may be null.
	 */
	public TupleDesc(Type[] typeAr, String[] fieldAr) {
		this.types = typeAr;
		this.fields = fieldAr;
	}

	public String[] getFieldArr(){
		return this.fields;
	}

	/**
	 * @return the number of fields in this TupleDesc
	 */
	public int numFields() {
		return fields.length;
	}
	public void setFields(String[] fi) {
		this.fields = fi;
	}

	/**
	 * Gets the (possibly null) field name of the ith field of this TupleDesc.
	 *
	 * @param i index of the field name to return. It must be a valid index.
	 * @return the name of the ith field
	 * @throws NoSuchElementException if i is not a valid field reference.
	 */
	public String getFieldName(int i) throws NoSuchElementException {
		//your code here
		if (i> fields.length-1 || i<0) {
			throw new NoSuchElementException("index out of range");
		}
		return fields[i];
	}

	/**
	 * Find the index of the field with a given name.
	 *
	 * @param name name of the field.
	 * @return the index of the field that is first to have the given name.
	 * @throws NoSuchElementException if no field with a matching name is found.
	 */
	public int nameToId(String name) throws NoSuchElementException {
		for (int a = 0 ; a < fields.length ; a++) {
			if (fields[a].equals(name)) {
				return a;		
			}
		}
		throw new NoSuchElementException();
	}

	/**
	 * Gets the type of the ith field of this TupleDesc.
	 *
	 * @param i The index of the field to get the type of. It must be a valid index.
	 * @return the type of the ith field
	 * @throws NoSuchElementException if i is not a valid field reference.
	 */
	public Type getType(int i) throws NoSuchElementException {
		if (i>=types.length||i<0) {
			throw new NoSuchElementException();
		}else {
			return types[i];
		}
	}

	/**
	 * @return The size (in bytes) of tuples corresponding to this TupleDesc.
	 * Note that tuples from a given TupleDesc are of a fixed size.
	 */
	public int getSize() {
		int result = 0;
		for (Type a : types) {
			if (a.equals(Type.STRING)) {
				result+=(129);
			}else {
				result += 4;
			}
		}
		return result;
	}

	/**
	 * Compares the specified object with this TupleDesc for equality.
	 * Two TupleDescs are considered equal if they are the same size and if the
	 * n-th type in this TupleDesc is equal to the n-th type in td.
	 *
	 * @param o the Object to be compared for equality with this TupleDesc.
	 * @return true if the object is equal to this TupleDesc.
	 */
	public boolean equals(Object o) {
		TupleDesc tp = (TupleDesc) o ;
		return Arrays.equals(tp.types, types);
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
	 * @return String describing this descriptor.
	 */
	public String toString() {
		StringBuffer sb=new StringBuffer("");
		for (int a = 0; a < types.length;a++) {
			sb.append(types[a]).append("(").append(fields[a]).append(")");
		}
		return sb.toString();
	}
}
