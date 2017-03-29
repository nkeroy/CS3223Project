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

    //int lcurs;    // Cursor for left side buffer
    //int rcurs;    // Cursor for right side buffer
    boolean eosl;  // Whether end of stream (left table) is reached
    boolean eosr;  // End of stream (right table)
    
    int lhcurs; // Cursor for left side hashmap
    int rhcurs; // right side hashmap
    int ltcurs; // curs for left tuple hashmap
    int rtcurs; // curs for right tuple hashmap
    
    int arrcurs; // array cursor for hashmap keys
    
    boolean eohj; // end of hash join
    
    
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

	//lcurs = 0; rcurs = 0;
       
        lhcurs = 0; rhcurs = 0;
        ltcurs = 0; rtcurs = 0;
        arrcurs = 0;
	eosl=false;
	eosr=false; // Changed to false from original code because doing hash here
    eohj=false;

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
	    rfname = "HJtemp-" + String.valueOf(filenum);
	    try{
		ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rfname));
		while( (rightpage = right.next()) != null){
		    out.writeObject(rightpage);
		}
		out.close();
	    }catch(IOException io){
		System.out.println("HashJoin:writing the temporay file error");
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

	int i,j;
	if(eohj){
		close();
	    return null;
	}
	outbatch = new Batch(batchsize);


	
        
        // Hash func written here, use prime no for both
        int iHash = 257; // Initial hash fn for partition phase
        int fHash = 251; // Final hash fn for partition phase
        //int iHash = numBuff;
        //int fHash = numBuff - 1;
        
        // Partition phase
        
        while (eosl==false) {
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
                    //System.out.println("left tuple added");
                }
            }
        }
        try {
            in = new ObjectInputStream(new FileInputStream(rfname));
            //eosr = false;
        } catch (IOException io) {
            System.err.println("NestedJoin:error in reading the file");
            System.exit(1);
        }
    
        // Do the same for the right table as well
        while (eosr==false){
            
            try{
                for (int m = 0; m < numBuff; m++) {
                    //System.out.println("Before right batch");
                    rightbatch = (Batch) in.readObject();
                    //System.out.println("After right batch");
                    if (rightbatch == null) {
                        eosr = true;
                        break;
                    }
                    for (int n = 0; n < rightbatch.size(); n++) {
                        
                        Tuple rtuple = rightbatch.elementAt(n);
                        int hKey = rtuple.dataAt(rightindex).hashCode() % iHash;
                        
                        if (rhash.containsKey(hKey)) {
                            rhash.get(hKey).add(rtuple);
                        } else {
                            ArrayList<Tuple> atup = new ArrayList<Tuple>();
                            atup.add(rtuple);
                            rhash.put(hKey, atup);
                        }
                        //System.out.println("right tuple added");
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
        
        /** Whenver a new left page came , we have to start the
         ** scanning of right table
         **/
        //try{
        //
        //in = new ObjectInputStream(new FileInputStream(rfname));
        //    eosr=false;
        //}catch(IOException io){
        //    System.err.println("NestedJoin:error in reading the file");
        //    System.exit(1);
        //}
    
        
        // Probing Phase:
        
        
        
        while (eohj == false) {
            // read in partition of left relation in hashmap, so lhash is the reference hashmap
            Object[] keyArray = lhash.keySet().toArray(); // Convert keySet into a form of an Object[]
            for (int o = arrcurs; o < keyArray.length; o++) {
                    if (rhash.containsKey(keyArray[o])) {
                        for (int b = ltcurs; b < lhash.get(keyArray[o]).size(); b++) {
                            for (int c = rtcurs; c < rhash.get(keyArray[o]).size(); c++) {
                                if ((lhash.get(keyArray[o]).get(b).dataAt(leftindex).hashCode() % fHash) == (rhash.get(keyArray[o]).get(b).dataAt(rightindex).hashCode() % fHash)) {
                                Tuple ltuple = lhash.get(keyArray[o]).get(b);
                                Tuple rtuple = rhash.get(keyArray[o]).get(c);
                                Tuple otuple = ltuple.joinWith(rtuple);
                                Debug.PPrint(otuple);
                                System.out.println();
                                outbatch.add(otuple);
                                    
                                    if (outbatch.isFull()) {
                                        if (ltcurs==lhash.get(keyArray[o]).size()-1 && rtcurs==rhash.get(keyArray[o]).size()-1) {
                                            arrcurs += 1;
                                            //lhash.remove(o);
                                            rtcurs = 0;
                                            ltcurs = 0;
                                            //break;
                                        } else if (!(ltcurs==lhash.get(keyArray[o]).size()-1) && rtcurs==rhash.get(keyArray[o]).size()-1) {
                                            ltcurs += 1;
                                            rtcurs = 0;
                                        } else if (ltcurs==lhash.get(keyArray[o]).size()-1 && !(rtcurs==rhash.get(keyArray[o]).size()-1)) {
                                            rtcurs += 1;
                                        } else {
                                            rtcurs += 1;
                                        }
                                        return outbatch;
                                    }
                                    
                                }
                            }
                        } // At the end of the partition, when all possible tuples are matched from left and right and outbatch not full:
                        //lhash.remove(o);
                        arrcurs += 1;
                        rtcurs = 0;
                        ltcurs = 0;
                    }
               
            }
            eohj = true; // end of hashjoin after scanning all keys in left hashmap
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











































