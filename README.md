New functions implemented in the code
1. *Block Nested Loops join*
   
   Refer to class file src/qp/operators/BlockNestedJoin
   
   This class will perform Block Nested Loops Joins of two relations and return joined tuples
   
   The implementation of this class is quite similar to the implementation of NestedJoin class.
   The main difference between these two classes is that BlockNestedJoin maintains an arraylist of Batch which is
   a block of pages. And there are two cursors "lcurs" and "lpgcurs" for left relation; "lcurs" is for traversing tuples
   in one input buffer while "lpgcurs" is for traversing buffer pages.
   
2. *External Sort*
   
   see src/qp/operators/Externalsort.java

   This class provides a external sort operation with one table input and one batch of sorted tuples as output.

   open() will construct a list of sorted runs and recursively merge them. In the end it will produce a single sorted run which contains the sorted table.

   next() will output one batch of the sorted table

   By passing different `asOrDes` variable, it can change the sort to be ascending or descending.

3. *SortedRun*
   
4. *Sort Merge join*

5. *Distinct*
   
   see src/qp/operators/Distinct.java
   
   This class provides a distinct operation and returns distinct tuples.
   
   open() will generate an external sort operator from the base to sort the given base table.
   
   next() will derive the distinct tuples from the external sort and output will be a batch of distinct tuples.

6. *Orderby*
   
   see src/qp/oprators/Orderby.java
   
   This class provides an orderby operation and returns tuples ordered by given attributes in Asc/Des order (depending on the input).
   
   open() will generate an external sort operator from the base to sort the given base table in Asc/Dec order based on given attributes.
   
   next() will output the sorted tuples in external sort.

7. *Aggregate functions (MIN, MAX, COUNT, AVG)*

   Aggregate functions are realized in Project class by modified open() method and next() method.
   
   In the new open() method, the operator will determine whether tuples or aggregate statistics need to projected and
   the outcome will be stored in a new boolean variable called hasAggregation.
   
   Then, for the modified next() method, it will call next_aggregation() method to generate aggregate statistics if
   hasAggregation is true and will call next_noaggregation() method to project wanted tuples if hasAggregation is false.
   
   For next_aggregation() method, it will initiate an arraylist of Aggregation object. The Aggregation class is
   implemented to store aggregate statistics of one attribute in a relation and there are methods in Aggregation class
   that are used to update statistics and return statistics to the caller. For the arraylist of Aggregation object,
   each entry of the arraylist corresponds to one attribute in the projection list. After scanning the whole relation,
   the next_aggregation() will return an Batch object called outbatch which stores wanted aggregate data to the consumer.
   
   For next_noaggregation() method, the function remains unchanged as the original next() method but some details are
   modified to fully utilize a Batch object called outbatch.

8. *Project functions* 

   In addition to implementations mentioned in Point 7, the next_noaggregation method() is written to fully utilize
   outbatch. One int variable called cursor and one boolean variable called eof are added to facilitate traversing the 
   relation. cursor is used to traverse tuples in one input buffer while eof represents whether the end of the relation 
   is reached. The outbatch will be returned to the consumer only when the outbatch is full or the end of the base 
   relation is reached.
