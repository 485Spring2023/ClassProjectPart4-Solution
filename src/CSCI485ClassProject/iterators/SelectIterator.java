package CSCI485ClassProject.iterators;

import CSCI485ClassProject.Cursor;
import CSCI485ClassProject.Iterator;
import CSCI485ClassProject.Records;
import CSCI485ClassProject.StatusCode;
import CSCI485ClassProject.models.AssignmentExpression;
import CSCI485ClassProject.models.ComparisonPredicate;
import CSCI485ClassProject.models.Record;
import com.apple.foundationdb.Transaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SelectIterator extends Iterator {
  private final Records records;

  private Transaction tx = null;

  private ComparisonPredicate compPredicate = null;
  private Cursor cursor = null;
  private final boolean isUsingIndex;

  private boolean isCursorInitialized = false;
  private boolean isIteratorReachToEOF = false;

  public SelectIterator(Records records, String tableName, ComparisonPredicate compPredicate, Iterator.Mode mode, boolean isUsingIndex) {
    /*
    * This implementation is designed for instructional purposes.
    * It emulates what should be implemented if a database designer was implementing the iterator. 
    * It returns a null iterator when isUsingIndex flag is TRUE and there is NO index on the referenced attribute of the compPredicate and the compPredicate consists of one attribute, e.g., Sal=30K.
    * It returns a valid iterator when isUsingIndex flag is TRUE and the compPredicate is either empty (no predicate is specified) or consists of a comparison of two attributes of a table, e.g., Sal=1000*age.
    * The second case with a valid iterator is an incorrect implementation.  It should return null because a query optimizer should not use the index structure for the purposes of such a predicate.
    * We are returning a valid iterator only for instruction purposes.  
    */
    this.records = records;
    this.compPredicate = compPredicate;
    this.isUsingIndex = isUsingIndex;
    this.setMode(mode);

    Cursor.Mode cursorMode = Cursor.Mode.READ;
    if (this.getMode() == Mode.READ_WRITE) {
      cursorMode = Cursor.Mode.READ_WRITE;
    }
    if (compPredicate.getPredicateType() == ComparisonPredicate.Type.NONE || compPredicate.getPredicateType() == ComparisonPredicate.Type.TWO_ATTRS) {
      // if its NONE predicate or predicate referencing two attributes, simply open a scan cursor
      cursor = records.openCursor(tableName, cursorMode);
    } else {
      // if its ONE_ATTR predicate, open a cursor that binds to a certain attribute
      cursor = records.openCursor(tableName, compPredicate.getLeftHandSideAttrName(), compPredicate.getRightHandSideValue(), compPredicate.getOperator(),
          cursorMode, isUsingIndex);
    }

    tx = cursor.getTx();
  }

  @Override
  public String getTableName() {
    return cursor.getTableName();
  }

  @Override
  public void resetToStart() {
    String tableName = getTableName();
    if (compPredicate.getPredicateType() == ComparisonPredicate.Type.NONE || compPredicate.getPredicateType() == ComparisonPredicate.Type.TWO_ATTRS) {
      // if its NONE predicate or predicate referencing two attributes, simply open a scan cursor
      cursor = records.openCursor(tableName, Cursor.Mode.READ);
    } else {
      // its an ONE_ATTR predicate, open a cursor that binds to a certain attribute
      cursor = records.openCursor(tableName, compPredicate.getLeftHandSideAttrName(), compPredicate.getRightHandSideValue(), compPredicate.getOperator(),
          Cursor.Mode.READ, isUsingIndex);
    }
    isIteratorReachToEOF = false;
    isCursorInitialized = false;
    cursor.setTx(tx);
  }

  @Override
  public Record next() {
    if (isIteratorReachToEOF) {
      return null;
    }

    Record currentRecord;
    if (!isCursorInitialized) {
      isCursorInitialized = true;
      currentRecord = records.getFirst(cursor);
    } else {
      currentRecord = records.getNext(cursor);
    }


    if (compPredicate.getPredicateType() == ComparisonPredicate.Type.TWO_ATTRS) {
      while (currentRecord != null && !compPredicate.isRecordQualified(currentRecord)) {
        currentRecord = records.getNext(cursor);
      }
    }

    if (currentRecord == null) {
      isIteratorReachToEOF = true;
//      close();
    }
    return currentRecord;
  }

  @Override
  public Transaction getTransaction() {
    return cursor.getTx();
  }


  @Override
  public void commit() {
    records.commitCursor(cursor);
  }

  @Override
  public void abort() {
    records.abortCursor(cursor);
  }

  @Override
  public StatusCode deleteRecord() {
    if (getMode() != Mode.READ_WRITE) {
      return StatusCode.ITERATOR_WRITE_NOT_SUPPORTED;
    }

    if (!isCursorInitialized) {
      return StatusCode.ITERATOR_NOT_POINTED_TO_ANY_RECORD;
    }

    return records.deleteRecord(cursor);
  }

  @Override
  public StatusCode updateRecord(AssignmentExpression assignExp) {
    if (getMode() != Mode.READ_WRITE) {
      return StatusCode.ITERATOR_WRITE_NOT_SUPPORTED;
    }

    if (!isCursorInitialized) {
      return StatusCode.ITERATOR_NOT_POINTED_TO_ANY_RECORD;
    }

    Record record = cursor.getCurrentRecord();
    Map<String, Object> updatedRes = assignExp.evaluate(record);
    List<String> attrs = new ArrayList<>(updatedRes.keySet());
    List<Object> attrVals = new ArrayList<>(updatedRes.values());

    return records.updateRecord(cursor, attrs.toArray(new String[0]), attrVals.toArray(new Object[0]));
  }
}
