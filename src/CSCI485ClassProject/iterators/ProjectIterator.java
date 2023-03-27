package CSCI485ClassProject.iterators;

import CSCI485ClassProject.Cursor;
import CSCI485ClassProject.Iterator;
import CSCI485ClassProject.Records;
import CSCI485ClassProject.fdb.FDBHelper;
import CSCI485ClassProject.models.Record;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;

import java.util.ArrayList;
import java.util.List;

import static CSCI485ClassProject.DBConf.TABLE_TEMP_STORE;

public class ProjectIterator extends Iterator {
  private Records records;

  private Transaction tx;

  private String attrName;

  // used when isDuplicateFree turns on
  private boolean isDuplicateFree;
  private DirectorySubspace tempDirectoryForDuplicateFree;
  private AsyncIterator<KeyValue> duplicateFreeKVIterator;
  private Tuple baseDuplicateFreeKeyPrefix;

  private boolean isIteratorReachToEOF = false;

  // the records may come from the iterator
  private Iterator sourceIterator = null;
  private boolean isCursorInitialized = false;
  private Cursor cursor = null;


  public ProjectIterator(Records records, String tableName, String attrName, boolean isDuplicateFree) {
    this.records = records;
    this.attrName = attrName;
    this.isDuplicateFree = isDuplicateFree;

    Cursor.Mode cursorMode = Cursor.Mode.READ;
    if (this.getMode() == Mode.READ_WRITE) {
      cursorMode = Cursor.Mode.READ_WRITE;
    }

    cursor = records.openCursor(tableName, cursorMode);
    tx = cursor.getTx();
    if (isDuplicateFree) {
      makeRecordsDuplicateFree();
    }
  }

  public ProjectIterator(Records records, Iterator sourceIterator, String attrName, boolean isDuplicateFree) {
    this.records = records;
    this.attrName = attrName;
    this.isDuplicateFree = isDuplicateFree;

    this.sourceIterator = sourceIterator;
    this.tx = sourceIterator.getTransaction();

    if (isDuplicateFree) {
      makeRecordsDuplicateFree();
    }
  }

  @Override
  public void resetToStart() {
    if (isDuplicateFree) {
      // reset the tempTable iterator
      duplicateFreeKVIterator = tx.getRange(Range.startsWith(tempDirectoryForDuplicateFree.pack(baseDuplicateFreeKeyPrefix))).iterator();
    } else {
      if (sourceIterator != null) {
        sourceIterator.resetToStart();
      } else {
        String tableName = cursor.getTableName();
        cursor = records.openCursor(tableName, Cursor.Mode.READ);
        cursor.setTx(tx);
      }
    }
    isIteratorReachToEOF = false;
    isCursorInitialized = false;
  }

  private Record nextRawRecord() {
    if (isIteratorReachToEOF) {
      return null;
    }

    Record res = null;
    if (isDuplicateFree && duplicateFreeKVIterator != null) {
      if (duplicateFreeKVIterator.hasNext()) {
        KeyValue kv = duplicateFreeKVIterator.next();
        Tuple keyTuple = tempDirectoryForDuplicateFree.unpack(kv.getKey());
        Object val = keyTuple.get(1);
        res = new Record();
        res.setAttrNameAndValue(attrName, val);
      }
    } else if (sourceIterator != null) {
      // record comes from another Iterator
      res = sourceIterator.next();
    } else {
      // records come from the cursor
      if (!isCursorInitialized) {
        isCursorInitialized = true;
        res = records.getFirst(cursor);
      } else {
        res = records.getNext(cursor);
      }
    }

    if (res == null) {
      if (!isDuplicateFree || duplicateFreeKVIterator != null) {
        isIteratorReachToEOF = true;
        close();
      }
    }
    return res;
  }



  @Override
  public Record next() {
    Record rawRecord = nextRawRecord();
    Record res = null;
    if (rawRecord == null) {
      return null;
    }

    if (isDuplicateFree) {
      // do the duplication checking with the previous record
      res = rawRecord;
    } else {
      // projects the required attribute out.
      while (rawRecord != null) {
        Object val = rawRecord.getValueForGivenAttrName(attrName);
        if (val == null) {
          // meaning that this record doesn't have the required attribute
          rawRecord = nextRawRecord();
        } else {
          res = new Record();
          res.setAttrNameAndValue(attrName, val);
          break;
        }
      }
    }

    return res;
  }

  @Override
  public Transaction getTransaction() {
    if (sourceIterator != null) {
      return sourceIterator.getTransaction();
    } else {
      return cursor.getTx();
    }
  }

  @Override
  public void close() {
    if (sourceIterator != null) {
      sourceIterator.close();
    } else if (cursor != null){
      records.commitCursor(cursor);
    }
  }

  @Override
  public String getTableName() {
    if (sourceIterator != null) {
      return sourceIterator.getTableName();
    } else {
      return cursor.getTableName();
    }
  }

  private void makeRecordsDuplicateFree() {
    // remove the temporary table first
    Database db = FDBHelper.initialization();

    // open a transaction to write attr in
    Transaction tx = FDBHelper.openTransaction(db);
    List<String> tempPath = new ArrayList<>();
    tempPath.add(TABLE_TEMP_STORE);

    // open the directory
    tempDirectoryForDuplicateFree = FDBHelper.createOrOpenSubspace(tx, tempPath);

    // the key is (attrName, attrVal), value is empty
    // key is identified by the transaction committed version
    baseDuplicateFreeKeyPrefix = new Tuple().add(tx.getReadVersion().join());
    Tuple valTuple = new Tuple();
    Record rawRecord = nextRawRecord();
    while (rawRecord != null) {
      Object val = rawRecord.getValueForGivenAttrName(attrName);
      if (val != null) {
        // put this val in the FDB
        Tuple keyTuple = baseDuplicateFreeKeyPrefix.addObject(val);
        tx.set(tempDirectoryForDuplicateFree.pack(keyTuple), valTuple.pack());
      }
      rawRecord = nextRawRecord();
    }

    FDBHelper.commitTransaction(tx);
    // open an iterator for that newly-added guys
    this.tx = FDBHelper.openTransaction(db);
    duplicateFreeKVIterator = this.tx.getRange(Range.startsWith(tempDirectoryForDuplicateFree.pack(baseDuplicateFreeKeyPrefix))).iterator();
  }



}
