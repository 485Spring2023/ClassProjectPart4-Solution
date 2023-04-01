package CSCI485ClassProject;

import CSCI485ClassProject.fdb.FDBHelper;
import CSCI485ClassProject.iterators.JoinIterator;
import CSCI485ClassProject.iterators.ProjectIterator;
import CSCI485ClassProject.iterators.SelectIterator;
import CSCI485ClassProject.models.AssignmentExpression;
import CSCI485ClassProject.models.ComparisonPredicate;
import CSCI485ClassProject.models.Record;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static CSCI485ClassProject.StatusCode.OPERATOR_INSERT_PRIMARY_KEYS_INVALID;
import static CSCI485ClassProject.StatusCode.OPERATOR_INSERT_RECORD_INVALID;
import static CSCI485ClassProject.StatusCode.PREDICATE_OR_EXPRESSION_VALID;
import static CSCI485ClassProject.StatusCode.SUCCESS;

// your codes
public class RelationalAlgebraOperatorsImpl implements RelationalAlgebraOperators {

  private Records records;
  private Indexes indexes;
  private TableManager tableManager;
  private Database dbHandle;

  public RelationalAlgebraOperatorsImpl() {
    Database db = FDBHelper.initialization();
    this.records = new RecordsImpl(db);
    this.indexes = new IndexesImpl();
    this.tableManager = new TableManagerImpl(db);
    dbHandle = db;
  }

  private Set<Record> getResultSetFromIterator(Iterator iterator) {
    Set<Record> res = new HashSet<>();

    while (true) {
      Record rec = iterator.next();
      if (rec == null) {
        break;
      }

      res.add(rec);
    }

    iterator.commit();
    return res;
  }

  private List<Record> getResultListFromIterator(Iterator iterator) {
    List<Record> res = new ArrayList<>();

    while (true) {
      Record rec = iterator.next();
      if (rec == null) {
        break;
      }

      res.add(rec);
    }

    iterator.commit();
    return res;
  }


  @Override
  public Iterator select(String tableName, ComparisonPredicate predicate, Iterator.Mode mode, boolean isUsingIndex) {
    if (predicate.validate() != StatusCode.PREDICATE_OR_EXPRESSION_VALID) {
      return null;
    }
    Iterator iterator = new SelectIterator(records, tableName, predicate, mode, isUsingIndex);
    return iterator;
  }

  @Override
  public Set<Record> simpleSelect(String tableName, ComparisonPredicate predicate, boolean isUsingIndex) {
    if (predicate.validate() != StatusCode.PREDICATE_OR_EXPRESSION_VALID) {
      return null;
    }
    Iterator iterator = select(tableName, predicate, Iterator.Mode.READ, isUsingIndex);
    return getResultSetFromIterator(iterator);
  }

  @Override
  public Iterator project(String tableName, String attrName, boolean isDuplicateFree) {

    Iterator iterator = new ProjectIterator(records, tableName, attrName, isDuplicateFree);
    return iterator;
  }

  @Override
  public Iterator project(Iterator iterator, String attrName, boolean isDuplicateFree) {
    Iterator resIte = new ProjectIterator(records, iterator, attrName, isDuplicateFree);
    return resIte;
  }

  @Override
  public List<Record> simpleProject(String tableName, String attrName, boolean isDuplicateFree) {
    Iterator iterator = project(tableName, attrName, isDuplicateFree);
    return getResultListFromIterator(iterator);
  }

  @Override
  public List<Record> simpleProject(Iterator iterator, String attrName, boolean isDuplicateFree) {
    Iterator resIte = project(iterator, attrName, isDuplicateFree);
    return getResultListFromIterator(resIte);
  }

  @Override
  public Iterator join(Iterator iterator1, Iterator iterator2, ComparisonPredicate predicate, Set<String> attrNames) {
    if (predicate.validate() != StatusCode.PREDICATE_OR_EXPRESSION_VALID) {
      return null;
    }
    Iterator iterator = new JoinIterator(iterator1, iterator2, predicate, attrNames);
    return iterator;
  }

  @Override
  public StatusCode insert(String tableName, Record record, String[] primaryKeys) {
    if (primaryKeys == null || primaryKeys.length == 0) {
      return OPERATOR_INSERT_PRIMARY_KEYS_INVALID;
    }

    Set<String> primaryKeySet = new HashSet<>();
    for (int i = 0; i < primaryKeys.length; i++) {
      String primaryKey = primaryKeys[i];
      primaryKeySet.add(primaryKey);
      if (record.getValueForGivenAttrName(primaryKey) == null) {
        return OPERATOR_INSERT_RECORD_INVALID;
      }
    }

    List<Object> primaryKeyVals = new ArrayList<>();
    Set<String> nonPKAttributes = new HashSet<>();
    List<Object> nonPKAttributeVals = new ArrayList<>();
    Map<String, Record.Value> attrMap = record.getMapAttrNameToValue();
    for (Map.Entry<String, Record.Value> attrEntry : attrMap.entrySet()) {
      String attrName = attrEntry.getKey();
      Object attrVal = attrEntry.getValue().getValue();
      if (!primaryKeySet.contains(attrName)) {
        // non-pk attribute
        nonPKAttributes.add(attrName);
        nonPKAttributeVals.add(attrVal);
      } else {
        primaryKeyVals.add(attrVal);
      }
    }

    // call records API
    return records.insertRecord(tableName, primaryKeySet.toArray(new String[0]), primaryKeyVals.toArray(new Object[0]),
        nonPKAttributes.toArray(new String[0]), nonPKAttributeVals.toArray(new Object[0]));
  }

  @Override
  public StatusCode update(String tableName, AssignmentExpression assignmentExpression, Iterator dataSourceIterator) {
    if (assignmentExpression.validate() != PREDICATE_OR_EXPRESSION_VALID) {
      return assignmentExpression.validate();
    }

    if (dataSourceIterator == null) {
      // update all records in the table
      dataSourceIterator = select(tableName, new ComparisonPredicate(), Iterator.Mode.READ_WRITE, false);
      if (dataSourceIterator == null) {
        return StatusCode.OPERATOR_UPDATE_ITERATOR_INVALID;
      }
    }

    if (!tableName.equals(dataSourceIterator.getTableName())) {
      return StatusCode.OPERATOR_UPDATE_ITERATOR_TABLENAME_UNMATCHED;
    }

    while (true) {
      Record record = dataSourceIterator.next();
      if (record == null) {
        break;
      }

      StatusCode status = dataSourceIterator.updateRecord(assignmentExpression);
      if (status != SUCCESS) {
        return status;
      }
    }

    dataSourceIterator.commit();
    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode delete(String tableName, Iterator dataSourceIterator) {
    if (dataSourceIterator == null) {
      dataSourceIterator = select(tableName, new ComparisonPredicate(), Iterator.Mode.READ_WRITE, false);
      if (dataSourceIterator == null) {
        return StatusCode.OPERATOR_DELETE_ITERATOR_INVALID;
      }
    }

    if (!tableName.equals(dataSourceIterator.getTableName())) {
      return StatusCode.OPERATOR_DELETE_ITERATOR_TABLENAME_UNMATCHED;
    }

    while (true) {
      Record record = dataSourceIterator.next();
      if (record == null) {
        break;
      }

      StatusCode status = dataSourceIterator.deleteRecord();
      if (status != SUCCESS) {
        return status;
      }
    }

    dataSourceIterator.commit();
    return StatusCode.SUCCESS;
  }

}
