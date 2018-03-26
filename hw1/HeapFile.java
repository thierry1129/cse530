package hw1;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A heap file stores a collection of tuples. It is also responsible for managing pages.
 * It needs to be able to manage page creation as well as correctly manipulating pages
 * when tuples are added or deleted.
 * @author Sam Madden modified by Doug Shook
 *
 */
public class HeapFile {

	public static final int PAGE_SIZE = 4096;
	private File f;
	private TupleDesc type;
	/**
	 * Creates a new heap file in the given location that can accept tuples of the given type
	 * @param f location of the heap file
	 * @param types type of tuples contained in the file
	 */
	public HeapFile(File f, TupleDesc type) {
		this.f = f;
		this.type = type;
	}

	public File getFile() {
		return f;
	}

	public TupleDesc getTupleDesc() {
		return type;
	}

	/**
	 * Creates a HeapPage object representing the page at the given page number.
	 * Because it will be necessary to arbitrarily move around the file, a RandomAccessFile object
	 * should be used here.
	 * @param id the page number to be retrieved
	 * @return a HeapPage at the given page number
	 * @throws IOException 
	 */
	public HeapPage readPage(int id)  {
		int begin = PAGE_SIZE* id;
		byte[] result = new byte[PAGE_SIZE];
		try {
			RandomAccessFile file = new RandomAccessFile(this.f, "r");
			
			file.seek(begin);
			file.readFully(result);
			file.close();
			return new HeapPage(id, result, getId());
		}catch(IOException e ) {
			e.printStackTrace();
		}
		return null;		
	}

	/**
	 * Returns a unique id number for this heap file. Consider using
	 * the hash of the File itself.
	 * @return
	 */
	public int getId() {
		return f.hashCode();
	}

	/**
	 * Writes the given HeapPage to disk. Because of the need to seek through the file,
	 * a RandomAccessFile object should be used in this method.
	 * @param p the page to write to disk
	 * @throws IOException 
	 */
	public void writePage(HeapPage p) throws IOException {
		RandomAccessFile file = new RandomAccessFile(this.f, "rws");
		int begin = PAGE_SIZE* p.getId();
		byte[] result = p.getPageData();
		file.seek(begin);
		file.write(result);
		file.close();
	}

	/**
	 * Adds a tuple. This method must first find a page with an open slot, creating a new page
	 * if all others are full. It then passes the tuple to this page to be stored. It then writes
	 * the page to disk (see writePage)
	 * @param t The tuple to be stored
	 * @return The HeapPage that contains the tuple
	 * @throws Exception 
	 */
	public HeapPage addTuple(Tuple t) throws Exception {	
		for (int m = 0 ; m <getNumPages() ; m++) {
			HeapPage hp=readPage(m);
			if (hp.getAvailSlots() == 0) {
				continue;
			}else {			
				hp.addTuple(t);
				this.writePage(hp);				
			}
		}	
		// now no avai page, need to add a new heappage 
		HeapPage newpg = new HeapPage(getNumPages(), new byte[PAGE_SIZE], getId());
		newpg.addTuple(t);
		this.writePage(newpg);
		return newpg;
	}

	/**
	 * This method will examine the tuple to find out where it is stored, then delete it
	 * from the proper HeapPage. It then writes the modified page to disk.
	 * @param t the Tuple to be deleted
	 * @throws Exception 
	 */
	public void deleteTuple(Tuple t) throws Exception{
		HeapPage hp = readPage(t.getPid());
		Iterator<Tuple> it = hp.iterator();
		while(it.hasNext()){
			if (it.next().getDesc().equals(t.getDesc())){
				hp.deleteTuple(t);
				this.writePage(hp);
				return;
			}
		}
		throw new Exception();
	}

	/**
	 * Returns an ArrayList containing all of the tuples in this HeapFile. It must
	 * access each HeapPage to do this (see iterator() in HeapPage)
	 * @return
	 */
	public ArrayList<Tuple> getAllTuples() {
		ArrayList<Tuple> result = new ArrayList<Tuple>();
		
		
		for (int a = 0; a < this.getNumPages(); a++){
			HeapPage hp = this.readPage(a);
			//hp.
			Iterator<Tuple> it = hp.iterator();
			
			while(it.hasNext()){
			
				result.add(it.next());
			}
		}
		return result;
	}

	/**
	 * Computes and returns the total number of pages contained in this HeapFile
	 * @return the number of pages
	 */
	public int getNumPages() {
		int result = (int)f.length()/PAGE_SIZE;
		return f.length()%PAGE_SIZE==0 ? result : result+1;
	}
}
