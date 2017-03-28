/** hash join algorithm **/

package qp.operators;

import qp.utils.*;
import java.io.*;
import java.util.*;
import java.lang.*;



public class HashJoin extends Join{


	int batchsize;  //Number of tuples per out batch

    /** The following fields are useful during execution of
     ** the HashJoin operation
     **/
    int leftindex;     // Index of the join attribute in left table
    int rightindex;    // Index of the join attribute in right table

    String rfname;    // The file name where the right table is materialize

    static int filenum=0;   // To get unique filenum for this operation

    Batch outbatch;   // Output buffer
    Batch leftbatch;  // Buffer for left input stream
    Batch rightbatch;  // Buffer for right input stream
    ObjectInputStream in; // File pointer to the right hand materialized file

    int lcurs;    // Cursor for left side buffer
    int rcurs;    // Cursor for right side buffer
    boolean eosl;  // Whether end of stream (left table) is reached
    boolean eosr;  // End of stream (right table)
    
    HashMap<Object, ArrayList<Tuple>> lhash; // hashtable for left input stream
    HashMap<Object, ArrayList<Tuple>> rhash; // hashtable for right input stream
    HashMap<Object, ArrayList<Tuple>> phash; // for probing
    

    public HashJoin(Join jn){
	super(jn.getLeft(),jn.getRight(),jn.getCondition(),jn.getOpType());
	schema = jn.getSchema();
	jointype = jn.getJoinType();
	numBuff = jn.getNumBuff();
    }


    /** During open finds the index of the join attributes
     **  Materializes the right hand side into a file
     **  Opens the connections
     **/



    public boolean open(){

		/** select number of tuples per batch **/
		int tuplesize=schema.getTupleSize();
		batchsize=Batch.getPageSize()/tuplesize;

	Attribute leftattr = con.getLhs();
	Attribute rightattr =(Attribute) con.getRhs();
	leftindex = left.getSchema().indexOf(leftattr);
	rightindex = right.getSchema().indexOf(rightattr);
	Batch rightpage;
        
    lhash = new HashMap<Object, ArrayList<Tuple>>();
    rhash = new HashMap<Object, ArrayList<Tuple>>();
    phash = new HashMap<Object, ArrayList<Tuple>>();

        
	/** initialize the cursors of input buffers **/

	lcurs = 0; rcurs = 0;
	eosl=false;
	/** because right stream is to be repetitively scanned
	 ** if it reached end, we have to start new scan
	 **/
	eosr=false; // Changed to false from original code because doing hash here

	/** Right hand side table is to be materialized
	 ** for the Nested join to perform
	 **/

	if(!right.open()){
	    return false;
	}else{
	    /** If the right operator is not a base table then
	     ** Materialize the intermediate result from right
	     ** into a file
	     **/

	    //if(right.getOpType() != OpType.SCAN){
	    filenum++;
	    rfname = "NJtemp-" + String.valueOf(filenum);
	    try{
		ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rfname));
		while( (rightpage = right.next()) != null){
		    out.writeObject(rightpage);
		}
		out.close();
	    }catch(IOException io){
		System.out.println("NestedJoin:writing the temporay file error");
		return false;
	    }
		//}
	    if(!right.close())
		return false;
	}
	if(left.open())
	    return true;
	else
	    return false;
    }



    /** from input buffers selects the tuples satisfying join condition
     ** And returns a page of output tuples
     **/


    public Batch next(){
	//System.out.print("NestedJoin:--------------------------in next----------------");
	//Debug.PPrint(con);
	//System.out.println();
	int i,j;
	if(eosl){
		close();
	    return null;
	}
	outbatch = new Batch(batchsize);


	while(!outbatch.isFull()){
        
        // Hash func written here, use prime no for both
        int iHash = 83; // Initial hash fn for partition phase
        int fHash = 97; // Final hash fn for partition phase
        //while (fHash == iHash)
         //   fHash = RandNumb.randInt(50,100); // Ensure that iHash and fHash is different
        
        
        // Partition phase
        
        if (eosl==false) {
            // Read m buffers of batches from left table and store in hashtable
            for (int m = 0; m < numBuff; m++) {
                Batch currBatch = left.next();
                if (currBatch == null) { // nothing left from the left table to be scanned
                    eosl=true;
                    break;
                }
                for (int n = 0; n < currBatch.size(); n++) {
         
                    Tuple ltuple = currBatch.elementAt(n);
                    int hKey = ltuple.dataAt(leftindex).hashCode() % iHash; // hash the search key to join
    
                    if (lhash.containsKey(hKey)) { // if key is already contained inside tbl
                        lhash.get(hKey).add(ltuple);
                    } else {
                        ArrayList<Tuple> atup = new ArrayList<Tuple>();
                        atup.add(ltuple);
                        lhash.put(hKey, atup);
                    }
                }
            }
        }
        // Do the same for the right table as well
        if (eosr==false){
            
            try{
                for (int m = 0; m < numBuff; m++) {
                    Batch currBatch = (Batch) in.readObject();
                    if (currBatch == null) {
                        eosr = true;
                        break;
                    }
                    for (int n = 0; n < currBatch.size(); n++) {
                        
                        Tuple rtuple = currBatch.elementAt(n);
                        int hKey = rtuple.dataAt(rightindex).hashCode() % iHash;
                        
                        if (rhash.containsKey(hKey)) {
                            rhash.get(hKey).add(rtuple);
                        } else {
                            ArrayList<Tuple> atup = new ArrayList<Tuple>();
                            atup.add(rtuple);
                            rhash.put(hKey, atup);
                        }
                        }
                    }
                
            } catch(EOFException e){
                try{
                    in.close();
                }catch (IOException io){
                    System.out.println("HashJoin:Error in temporary file reading");
                }
                eosr=true;
            }catch(ClassNotFoundException c){
                System.out.println("HashJoin:Some error in deserialization ");
                System.exit(1);
            }catch(IOException io){
                System.out.println("HashJoin:temporary file reading error");
                System.exit(1);
            }
        }
    
        // Probing Phase:
        
        while (!lhash.isEmpty()) { // use left relation as reference
            // read in partition of left relation in hashmap
            for (Object o : lhash.keySet()) {
                //for (int a = 0; a < numBuff - 1; a++) {
                    // the key index in lhash should match in rhash
                    if (rhash.containsKey(o)) {
                        for (int b = 0; b < lhash.get(o).size(); b++) {
                            for (int c = 0; c < rhash.get(o).size(); c++) {
                                if ((lhash.get(o).get(b).dataAt(leftindex).hashCode() % fHash) == (rhash.get(o).get(b).dataAt(rightindex).hashCode() % fHash)) {
                                Tuple ltuple = lhash.get(o).get(b);
                                Tuple rtuple = rhash.get(o).get(c);
                                Tuple otuple = ltuple.joinWith(rtuple);
                                Debug.PPrint(otuple);
                                System.out.println();
                                outbatch.add(otuple);
                                }
                            }
                        }
                                
                    }
                }
            //}
                
        }
            
        
	    
		/** Whenver a new left page came , we have to start the
		 ** scanning of right table
		 **/
		try{

		    in = new ObjectInputStream(new FileInputStream(rfname));
		    eosr=false;
		}catch(IOException io){
		    System.err.println("NestedJoin:error in reading the file");
		    System.exit(1);
		}

	    }

	    
	return outbatch;
    }



    /** Close the operator */
    public boolean close(){

	File f = new File(rfname);
	f.delete();
	return true;

    }


}











































