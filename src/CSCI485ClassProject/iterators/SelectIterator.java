package CSCI485ClassProject.iterators;

import CSCI485ClassProject.Cursor;
import CSCI485ClassProject.Iterator;
import CSCI485ClassProject.Records;
import CSCI485ClassProject.models.ComparisonPredicate;
import CSCI485ClassProject.models.Record;
import com.apple.foundationdb.Transaction;

public class SelectIterator extends Iterator {
  private final Records records;

  private ComparisonPredicate compPredicate = null;
  private Cursor cursor = null;
  private boolean isUsingIndex;

  private boolean isCursorInitialized = false;
  private boolean isIteratorReachEOF = false;
  private Record currentRecord;

  public SelectIterator(Records records, String tableName, ComparisonPredicate compPredicate, boolean isUsingIndex) {
    this.records = records;
    this.compPredicate = compPredicate;
    this.isUsingIndex = isUsingIndex;

    if (compPredicate.getPredicateType() == ComparisonPredicate.Type.NONE || compPredicate.getPredicateType() == ComparisonPredicate.Type.TWO_ATTR) {
      // if its NONE predicate or predicate referencing two attributes, simply open a scan cursor
      cursor = records.openCursor(tableName, Cursor.Mode.READ);
    } else {
      // its an ONE_ATTR predicate, open a cursor that binds to a certain attribute
      cursor = records.openCursor(tableName, compPredicate.getLeftHandSideAttrName(), compPredicate.getRightHandSideValue(), compPredicate.getOperator(),
          Cursor.Mode.READ, isUsingIndex);
    }
  }



  @Override
  public Record next() {
    if (isIteratorReachEOF) {
      return null;
    }

    if (!isCursorInitialized) {
      isCursorInitialized = true;
      currentRecord = records.getFirst(cursor);
    } else {
      currentRecord = records.getNext(cursor);
    }


    if (compPredicate.getPredicateType() == ComparisonPredicate.Type.TWO_ATTR) {
      while (currentRecord != null && !compPredicate.isRecordQualified(currentRecord)) {
        currentRecord = records.getNext(cursor);
      }
    }

    if (currentRecord == null) {
      isIteratorReachEOF = true;
      close();
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

}
