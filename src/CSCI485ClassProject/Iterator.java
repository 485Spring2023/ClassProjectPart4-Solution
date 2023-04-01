package CSCI485ClassProject;

import CSCI485ClassProject.models.AssignmentExpression;
import CSCI485ClassProject.models.Record;
import com.apple.foundationdb.Transaction;

public abstract class Iterator {

  public enum Mode {
    READ,
    READ_WRITE
  }

  private Mode mode;

  public Mode getMode() {
    return mode;
  };

  public void setMode(Mode mode) {
    this.mode = mode;
  };

  public abstract Record next();

  public abstract String getTableName();

  public abstract Transaction getTransaction();

  public StatusCode deleteRecord() {
    return StatusCode.ITERATOR_WRITE_NOT_SUPPORTED;
  };

  public StatusCode updateRecord(AssignmentExpression assignExp) {
    return StatusCode.ITERATOR_WRITE_NOT_SUPPORTED;
  };

  public abstract void commit();

  public abstract void abort();

  public abstract void resetToStart();
}
