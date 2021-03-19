/**
 * Sort Merge Join Algorithm
 */

package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.Tuple;

import java.util.ArrayList;

public class SortMergeJoin extends Join {

    int batchsize;                  // Number of tuples per out batch
    int leftbatchsize;
    int rightbatchsize;
    ArrayList<Integer> leftindexes;   // Indices of the join attributes in left table
    ArrayList<Integer> rightindexes;  // Indices of the join attributes in right table
    Batch outbatch;                 // Buffer page for output
    ArrayList<Batch> leftbatches;
    ArrayList<Batch> rightbatches;

    int lcurs;                      // Cursor for left side buffer
    int rcurs;                      // Cursor for right side buffer
    int lpgcurs;
    int rpgcurs;
    boolean eosl;
    boolean eosr;

    boolean hasRepeat;
    Tuple leftBaseTuple;
    Tuple tempTuple;
    ArrayList<Tuple> repeatedTuples;
    ArrayList<Tuple> tuplesToOutput;

    ExternalSort leftES;
    ExternalSort rightES;

    public SortMergeJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getConditionList(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }

    /**
     * During open finds the index of the join attributes
     * * Materializes the right hand side into a file
     * * Opens the connections
     **/
    public boolean open() {
        /** select number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;
        leftbatchsize = Batch.getPageSize() / left.getSchema().getTupleSize();
        rightbatchsize = Batch.getPageSize() / right.getSchema().getTupleSize();

        /** find indices attributes of join conditions **/
        leftindexes = new ArrayList<>();
        rightindexes = new ArrayList<>();
        for (Condition con : conditionList) {
            Attribute leftattr = con.getLhs();
            Attribute rightattr = (Attribute) con.getRhs();
            leftindexes.add(left.getSchema().indexOf(leftattr));
            rightindexes.add(right.getSchema().indexOf(rightattr));
        }

        leftES = new ExternalSort(OpType.SORT, left, leftindexes, numBuff, ExternalSort.ASCENDING);
        rightES = new ExternalSort(OpType.SORT, right, rightindexes, numBuff, ExternalSort.ASCENDING);

        if (!leftES.open()) return false;
        if (!rightES.open()) return false;

        if (numBuff % 2 == 0) {
            leftbatches = new ArrayList<>();
            rightbatches = new ArrayList<>();
            for (int i = 0; i < numBuff/2-1; ++i) {
                leftbatches.add(null);
                rightbatches.add(null);
            }
            leftbatches.add(null);
        } else {
            leftbatches = new ArrayList<>();
            rightbatches = new ArrayList<>();
            for (int i = 0; i < numBuff/2; ++i) {
                leftbatches.add(null);
                rightbatches.add(null);
            }
        }
        lpgcurs = 0;
        rpgcurs = 0;
        lcurs = 0;
        rcurs = 0;
        eosl = false;
        eosr = false;
        hasRepeat = false;
        repeatedTuples = new ArrayList<>();
        tuplesToOutput = new ArrayList<>();

        for (int i = 0; i < leftbatches.size(); ++i) {
            Batch curr = leftES.next();
            if (curr == null) {
                break;
            }
            leftbatches.set(i, curr);
        }
        lpgcurs = 0; lcurs = 0;
        for (int i = 0; i < rightbatches.size(); ++i) {
            Batch curr = rightES.next();
            if (curr == null) {
                break;
            }
            rightbatches.set(i, curr);
        }
        rpgcurs = 0; rcurs = 0;

        return true;
    }

    /**
     * from input buffers selects the tuples satisfying join condition
     * * And returns a page of output tuples
     **/
    public Batch next() {
        outbatch = new Batch(batchsize);
        while (!outbatch.isFull()) {
            if (lpgcurs == leftbatches.size()) {
                for (int i = 0; i < leftbatches.size(); ++i) {
                    leftbatches.set(i, null);
                }
                for (int i = 0; i < leftbatches.size(); ++i) {
                    Batch curr = leftES.next();
                    if (curr == null) {
                        break;
                    }
                    leftbatches.set(i, curr);
                }
                lpgcurs = 0; lcurs = 0;
            }
            if (rpgcurs == rightbatches.size()) {
                for (int i = 0; i < rightbatches.size(); ++i) {
                    rightbatches.set(i, null);
                }
                for (int i = 0; i < rightbatches.size(); ++i) {
                    Batch curr = rightES.next();
                    if (curr == null) {
                        break;
                    }
                    rightbatches.set(i, curr);
                }
                rpgcurs = 0; rcurs = 0;
            }

            if (leftbatches.get(lpgcurs) == null || leftbatches.get(lpgcurs).size() == 0) eosl = true;
            if (rightbatches.get(rpgcurs) == null || rightbatches.get(rpgcurs).size() == 0) eosr = true;

            if (eosl || eosr) {
                if (outbatch.size() == 0) return null;
                else return outbatch;
            } else {
                Tuple lefttuple = leftbatches.get(lpgcurs).get(lcurs);
                Tuple righttuple = rightbatches.get(rpgcurs).get(rcurs);

                if (hasRepeat && !allEqual(leftBaseTuple, lefttuple)) {
                    if (Tuple.compareTuples(lefttuple, tempTuple, leftindexes, rightindexes) == 0) {
                        for (Tuple t : repeatedTuples) {
                            tuplesToOutput.add(lefttuple.joinWith(t));
                        }
                    } else if (Tuple.compareTuples(lefttuple, tempTuple, leftindexes, rightindexes) > 0) {
                        hasRepeat = false;
                    }
                }

                if (tuplesToOutput.size() != 0) {
                    outbatch.add(tuplesToOutput.get(0));
                    tuplesToOutput.remove(0);
                    continue;
                }

                int cmpRes = Tuple.compareTuples(lefttuple, righttuple, leftindexes, rightindexes);
                if (cmpRes == 0) {
                    Tuple outTuple = lefttuple.joinWith(righttuple);
                    outbatch.add(outTuple);
                    if (!hasRepeat) {
                        leftBaseTuple = lefttuple;
                        repeatedTuples = new ArrayList<>();
                        repeatedTuples.add(righttuple);
                        tempTuple = repeatedTuples.get(0);
                        hasRepeat = true;
                    } else if (Tuple.compareTuples(righttuple, tempTuple, rightindexes, rightindexes) == 0) {
                        repeatedTuples.add(righttuple);
                    }
                    rcurs++;
                    if (rcurs == rightbatches.get(rpgcurs).size()) {
                        rcurs = 0;
                        rpgcurs++;
                    }
                } else if (cmpRes < 0) {
                    lcurs++;
                    if (lcurs == leftbatches.get(lpgcurs).size()) {
                        lcurs = 0;
                        lpgcurs++;
                    }
                } else {
                    rcurs++;
                    if (rcurs == rightbatches.get(rpgcurs).size()) {
                        rcurs = 0;
                        rpgcurs++;
                    }
                }
            }
        }

        return outbatch;
    }

    /**
     * Close the operator
     */
    public boolean close() {
        if (!leftES.close()) return false;
        if (!rightES.close()) return false;
        return true;
    }

    public  boolean allEqual(Tuple left, Tuple right) {
        if (left.data().size() != right.data().size())
            return false;
        int indexesSize = left.data().size();
        ArrayList<Integer> compareIndexes = new ArrayList<>();
        for (int i = 0; i < indexesSize; ++i) {
            compareIndexes.add(i);
        }
        return Tuple.compareTuples(left, right, compareIndexes, compareIndexes) == 0;
    }
}
