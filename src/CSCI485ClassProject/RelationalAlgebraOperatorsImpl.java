package CSCI485ClassProject;

import CSCI485ClassProject.fdb.FDBHelper;
import CSCI485ClassProject.iterators.JoinIterator;
import CSCI485ClassProject.iterators.ProjectIterator;
import CSCI485ClassProject.iterators.SelectIterator;
import CSCI485ClassProject.models.AssignmentPredicate;
import CSCI485ClassProject.models.ComparisonPredicate;
import CSCI485ClassProject.models.Record;
import com.apple.foundationdb.Database;

import java.util.HashSet;
import java.util.Set;

// your codes
public class RelationalAlgebraOperatorsImpl implements RelationalAlgebraOperators {

  private Records records;
  private Indexes indexes;
  private TableManager tableManager;
  private Database dbHandle;

  public RelationalAlgebraOperatorsImpl(TableManager tableManager, Records records, Indexes indexes) {
    this.records = records;
    this.indexes = indexes;
    this.tableManager = tableManager;
    dbHandle = FDBHelper.initialization();
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

    return res;
  }

  @Override
  public Iterator select(String tableName, ComparisonPredicate predicate, boolean isUsingIndex) {
    if (predicate.validate() != StatusCode.PREDICATE_VALID) {
      return null;
    }
    Iterator iterator = new SelectIterator(records, tableName, predicate, isUsingIndex);
    return iterator;
  }

  @Override
  public Set<Record> simpleSelect(String tableName, ComparisonPredicate predicate, boolean isUsingIndex) {
    if (predicate.validate() != StatusCode.PREDICATE_VALID) {
      return null;
    }
    Iterator iterator = select(tableName, predicate, isUsingIndex);
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
  public Set<Record> simpleProject(String tableName, String attrName, boolean isDuplicateFree) {
    Iterator iterator = project(tableName, attrName, isDuplicateFree);
    return getResultSetFromIterator(iterator);
  }

  @Override
  public Set<Record> simpleProject(Iterator iterator, String attrName, boolean isDuplicateFree) {
    Iterator resIte = project(iterator, attrName, isDuplicateFree);
    return getResultSetFromIterator(resIte);
  }

  @Override
  public Iterator join(Iterator iterator1, Iterator iterator2, ComparisonPredicate predicate, Set<String> attrNames) {
    if (predicate.validate() != StatusCode.PREDICATE_VALID) {
      return null;
    }
    Iterator iterator = new JoinIterator(iterator1, iterator2, predicate, attrNames);
    return iterator;
  }

  @Override
  public StatusCode insert(String tableName, Cursor cursor, boolean isGettingLast) {
    return null;
  }

  @Override
  public StatusCode update(String tableName, AssignmentPredicate assignPredicate, ComparisonPredicate compPredicate) {
    return null;
  }

  @Override
  public StatusCode delete(String tableName, ComparisonPredicate comparisonPredicate) {
    return null;
  }
}
