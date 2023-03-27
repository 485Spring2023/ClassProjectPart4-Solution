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

  public SelectIterator(Records records, String tableName, ComparisonPredicate compPredicate, boolean isUsingIndex) {
    this.records = records;
    this.compPredicate = compPredicate;
    this.isUsingIndex = isUsingIndex;

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
  public void close() {
    records.commitCursor(cursor);
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
