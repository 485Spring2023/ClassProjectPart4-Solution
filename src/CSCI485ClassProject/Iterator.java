package CSCI485ClassProject;

import CSCI485ClassProject.models.ComparisonPredicate;
import CSCI485ClassProject.models.Record;
import com.apple.foundationdb.Transaction;

public class Iterator {

  public enum IteratorType {
    SELECT,
    PROJECT,
    JOIN
  }
  // iterator type
  private IteratorType iteratorType;

  // used by select
  private ComparisonPredicate selectCompPredicate = null;
  private Cursor selectCursor = null;

  // used by the project operator
  private Cursor projectCursor = null;

  // used by join operator
  private Cursor joinOuterCursor = null;
  private Cursor joinInnerCursor = null;
  private String[] joinAttributesToProject = null; // non-null meaning the join operator projects certain attributes


  public Record next() {return null;}

  public Transaction getTransaction() {return null;}

  public String getTableName() {return "";}

  public void close() {}

  // copy an iterator that seeks just initialized
  public void resetToStart() {
    // TODO: override this method for all iterators
  }
}
