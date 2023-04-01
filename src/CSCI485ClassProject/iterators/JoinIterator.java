package CSCI485ClassProject.iterators;

import CSCI485ClassProject.Iterator;
import CSCI485ClassProject.models.ComparisonPredicate;
import CSCI485ClassProject.models.Record;
import com.apple.foundationdb.Transaction;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class JoinIterator extends Iterator {
  // used by join operator
  private Iterator outerIterator = null;
  private Record currentOuterRecord = null;

  private Iterator innerIterator = null;
  private ComparisonPredicate joinPredicate;
  private Set<String> joinAttributesToProject = null; // non-null meaning the join operator projects certain attributes

  private boolean isIteratorReachToEOF = false;

  public JoinIterator(Iterator outerIterator, Iterator innerIterator, ComparisonPredicate predicate, Set<String> attrNames) {
    this.outerIterator = outerIterator;
    this.innerIterator = innerIterator;
    this.joinPredicate = predicate;
    this.joinAttributesToProject = attrNames;
  }

  private Record joinTwoRecord(Record rec1, Record rec2) {
    // e.g. Employee: rec1(SSN: 1, Name: Bob, Salary: 1500, Dno: 2),
    // Department: rec2(Name: CompScience, Dno: 2)
    // after join: (SSN: 1, Employee.Name: Bob, Salary: 1500, Employee.Dno: 2, Department.Name: CompScience, Department.Dno: 2)
    Record res = new Record();

    HashMap<String, Record.Value> attrValMap1 = rec1.getMapAttrNameToValue();
    HashMap<String, Record.Value> attrValMap2 = rec2.getMapAttrNameToValue();

    Set<String> attrSet1 = attrValMap1.keySet();
    Set<String> attrSet2 = attrValMap2.keySet();

    Set<String> commonAttrNames = new HashSet<>(attrSet1);
    commonAttrNames.retainAll(attrSet2);

    for (String attrName : attrSet1) {
      if (joinAttributesToProject != null && !joinAttributesToProject.contains(attrName)) {
        continue;
      }
      Object attrVal = rec1.getValueForGivenAttrName(attrName);
      if (!commonAttrNames.contains(attrName)) {
        res.setAttrNameAndValue(attrName, attrVal);
      } else {
        String newAttrName = outerIterator.getTableName() + "." + attrName;
        res.setAttrNameAndValue(newAttrName, attrVal);
      }
    }

    for (String attrName : attrSet2) {
      if (joinAttributesToProject != null && !joinAttributesToProject.contains(attrName)) {
        continue;
      }
      Object attrVal = rec2.getValueForGivenAttrName(attrName);
      if (!commonAttrNames.contains(attrName)) {
        res.setAttrNameAndValue(attrName, attrVal);
      } else {
        String newAttrName = innerIterator.getTableName() + "." + attrName;
        res.setAttrNameAndValue(newAttrName, attrVal);
      }
    }

    return res;
  }

  @Override
  public Record next() {
    if (isIteratorReachToEOF) {
      return null;
    }

    while (true) {
      if (currentOuterRecord == null) {
        currentOuterRecord = outerIterator.next();
      }

      Record innerRec = innerIterator.next();
      if (innerRec == null) {
        // move the outerRecord to the next when the inner loop reach to EOF
        currentOuterRecord = outerIterator.next();
        innerIterator.resetToStart();
        innerRec = innerIterator.next();
      }
      if (currentOuterRecord == null || innerRec == null) {
        isIteratorReachToEOF = true;
        return null;
      }

      if (joinPredicate.isRecordQualified(currentOuterRecord, innerRec)) {
        return joinTwoRecord(currentOuterRecord, innerRec);
      }
    }
  }

  @Override
  public String getTableName() {
    return outerIterator.getTableName() + "." + innerIterator.getTableName();
  }

  @Override
  public Transaction getTransaction() {
    return null;
  }

  @Override
  public void resetToStart() {
    outerIterator.resetToStart();
    innerIterator.resetToStart();
    currentOuterRecord = null;
    isIteratorReachToEOF = false;
  }

  @Override
  public void commit() {
    outerIterator.commit();
    innerIterator.commit();
  }

  @Override
  public void abort() {
    outerIterator.abort();
    innerIterator.abort();
  }
}
