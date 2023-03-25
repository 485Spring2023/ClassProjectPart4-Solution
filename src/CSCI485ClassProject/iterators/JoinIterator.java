package CSCI485ClassProject.iterators;

import CSCI485ClassProject.Iterator;
import CSCI485ClassProject.models.ComparisonPredicate;
import CSCI485ClassProject.models.Record;

public class JoinIterator extends Iterator {
  // used by join operator
  private Iterator outerIterator = null;
  private Record currentOuterRecord = null;

  private Iterator innerIterator = null;
  private ComparisonPredicate joinPredicate;
  private String[] joinAttributesToProject = null; // non-null meaning the join operator projects certain attributes

  private boolean isJoinIteratorReachToEOF = false;

  public JoinIterator(Iterator outerIterator, Iterator innerIterator, ComparisonPredicate predicate, String[] attrNames) {
    this.outerIterator = outerIterator;
    this.innerIterator = innerIterator;
    this.joinPredicate = predicate;
    this.joinAttributesToProject = attrNames;
  }

  private Record joinTwoRecord(Record rec1, Record rec2) {
    // TODO
    return null;
  }

  @Override
  public Record next() {
    if (isJoinIteratorReachToEOF) {
      return null;
    }

    while (true) {
      if (currentOuterRecord == null) {
        currentOuterRecord = outerIterator.next();
      }

      Record innerRec = innerIterator.next();
      if (innerRec == null) {
        // move the outerRecord when the inner loop reachs to EOF
        currentOuterRecord = outerIterator.next();
        innerIterator.resetToStart();
        innerRec = innerIterator.next();
      }
      if (currentOuterRecord == null || innerRec == null) {
        isJoinIteratorReachToEOF = true;
        return null;
      }

      if (joinPredicate.isRecordQualified(currentOuterRecord, innerRec)) {
        return joinTwoRecord(currentOuterRecord, innerRec);
      }
    }
  }

  @Override
  public String getTableName() {
    return outerIterator.getTableName() + "@" + innerIterator.getTableName();
  }

}
