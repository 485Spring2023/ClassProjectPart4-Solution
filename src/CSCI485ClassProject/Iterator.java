package CSCI485ClassProject;

import CSCI485ClassProject.models.AssignmentExpression;
import CSCI485ClassProject.models.Record;
import com.apple.foundationdb.Transaction;

public class Iterator {

  public enum Mode {
    READ,
    READ_WRITE
  }

  private Mode mode = Mode.READ;

  public Mode getMode() {
    return mode;
  }

  public void setMode(Mode mode) {
    this.mode = mode;
  }

  public Record next() {return null;}

  public String getTableName() {
    return "";
  }

  public Transaction getTransaction() {return null;}

  public StatusCode deleteRecord() {
    return StatusCode.ITERATOR_WRITE_NOT_SUPPORTED;
  }

  public StatusCode updateRecord(AssignmentExpression assignExp) {
    return StatusCode.ITERATOR_WRITE_NOT_SUPPORTED;
  }

  public void close() {}

  // seeks just initialized
  public void resetToStart() {}
}
