package qp.operators;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;

public class BlockNestedJoin extends Join {
	int batchSize; // number of tuples per batch

	int leftIndex; // index of join attribute in left table
	int rightIndex; // index of join attribute in right table
	Object searchKey; // the searck key/PK of the tuple

	String rfname; // file name of right table

	static int filenum = 0; // unique filenum for this operation

	Batch outputBatch; // output buffer
	Batch leftBatch; // buffer for left input stream
	Batch rightBatch; // buffer for right input stream
	ObjectInputStream in; /// file pointer to the right materialized file

	int rightCursor; // pointer for right side buffer
	HashMap<Object, Tuple> outerTableHashMap; // hashmap to store the outer table
	boolean eosr; // end of stream(right table)
	boolean eosl; // end of stream(left table)
	boolean eobj; // end of the block nesteed join
	
	int outerTupleTracker; //track the status of outer relation
	public BlockNestedJoin(Join jn) {
		super(jn.getLeft(), jn.getRight(), jn.getCondition(), jn.getOpType());
		schema = jn.getSchema();
		jointype = jn.getJoinType();
		numBuff = jn.getNumBuff();
	}

	public boolean open() {
		/** hashmap that will be used to store the outer relation tuples**/
		outerTableHashMap = new HashMap<Object, Tuple>();
		/** select number of tuples per batch **/
		int tupleSize = schema.getTupleSize();
		batchSize = Batch.getPageSize() / tupleSize;
		/** get the comparison Key from both table**/
		Attribute leftattr = con.getLhs();
		Attribute rightattr = (Attribute) con.getRhs();
		leftIndex = left.getSchema().indexOf(leftattr);
		rightIndex = right.getSchema().indexOf(rightattr);

		/** initialize the cursors of input buffers **/
		Batch rightpage;
		rightCursor = 0;
		outerTupleTracker =0;
		eobj = false;
		eosl = false;
		eosr = true; // right stream is to be repetitively scanned
		if (!right.open()) {
			return false;
		} else {
			/**
			 * If the right operator is not a base table then Materialize the
			 * intermediate result from right into a file
			 **/

			// if(right.getOpType() != OpType.SCAN){
			filenum++;
			rfname = "NJtemp-" + String.valueOf(filenum);
			try {
				ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rfname));
				while ((rightpage = right.next()) != null) {
					out.writeObject(rightpage);
				}
				out.close();
			} catch (IOException io) {
				System.out.println("NestedJoin:writing the temporay file error");
				return false;
			}
			// }
			if (!right.close()) {
				return false;
			}
		}
		if (left.open()) {
			return true;
		} else {
			return false;
		}
	}

	public Batch next() {
		int r;
		if (eobj) {
			close();
			return null;
		}

		outputBatch = new Batch(batchSize);
		while (!outputBatch.isFull() && eobj == false) {
			/**
			 * if inner relation has reach end of file but left relation still have data to read
			 * read a new set of batches of tuple from outer relation
			 **/
			if (eosr == true && eosl == false) {
				/**
				 * Read N-1 buffer worth of batches from left relation
				 * and store them in a hashmap
				 */
				for (int i = 0; i < (numBuff - 1); i++) {
					Batch outerBatch = left.next();
					if (outerBatch == null) {
						eosl = true;
						break;
					}
					for (int j = 0; j < outerBatch.size(); j++) {
						/** 
						 * store the tuple in the hashmap using the searchKey as a key vlue
						 * **/
						System.out.println("Loading tuples no : " + ++outerTupleTracker + " into memory");
						Tuple outerTuple = outerBatch.elementAt(j);
						searchKey = outerTuple.dataAt(leftIndex);
						outerTableHashMap.put(searchKey, outerTuple);
					}
				}
				try {
					in = new ObjectInputStream(new FileInputStream(rfname));
					eosr = false;
				} catch (IOException io) {
					System.err.println("NestedJoin:error in reading the file");
					System.exit(1);
				}
			}
			while (eosr == false) {
				try {
					if (rightCursor == 0) {
						rightBatch = (Batch) in.readObject();
					}
					for (r = rightCursor; r < rightBatch.size(); r++) {
						Tuple rightTuple = rightBatch.elementAt(r);
						Object rightSearchKey = rightTuple.dataAt(rightIndex);
						//using the searchKey of the inner tuple, check if the outertable has the same searchKey
						if (outerTableHashMap.containsKey(rightSearchKey)) {
							Tuple leftTuple = outerTableHashMap.get(rightSearchKey);
							Tuple outputTuple = leftTuple.joinWith(rightTuple);
							Debug.PPrint(outputTuple);
							System.out.println();
							outputBatch.add(outputTuple);
							if (outputBatch.isFull()) {
								if (r != rightBatch.size() - 1) { // case 1: outputBatch is full but inner batch is not read finish completely
									rightCursor = r + 1;
								} else {
									rightCursor = 0; // other case; the current inner batch has been read finished
								}
								return outputBatch;
							}
						}
					}
					rightCursor = 0;
				} catch (EOFException e) {
					try {
						in.close();
					} catch (IOException io) {
						System.out.println("NestedJoin:Error in temporary file reading");
					}
					//if both outer and inner relation has reach end of file, end the block jon
					if (eosl) {
						eobj = true;
					}
					
					//end of right stream, clear the hashmap to get a new set of batches of tuples from other relation
					eosr = true;
					outerTableHashMap.clear();
				} catch (ClassNotFoundException c) {
					System.out.println("NestedJoin:Some error in deserialization ");
					System.exit(1);
				} catch (IOException io) {
					System.out.println("NestedJoin:temporary file reading error");
					System.exit(1);
				}
			}
		}
		return outputBatch;

	}

	/** Close the operator */
	public boolean close() {

		File f = new File(rfname);
		f.delete();
		return true;
	}
}
