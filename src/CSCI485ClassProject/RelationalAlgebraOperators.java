package CSCI485ClassProject;

import CSCI485ClassProject.models.AssignmentExpression;
import CSCI485ClassProject.models.ComparisonPredicate;
import CSCI485ClassProject.models.Record;

import java.util.Set;

public interface RelationalAlgebraOperators {
  Iterator select(String tableName, ComparisonPredicate predicate, Iterator.Mode mode, boolean isUsingIndex);

  Set<Record> simpleSelect(String tableName, ComparisonPredicate predicate, boolean isUsingIndex);


  // if isDuplicateFree is true, use sorting NO in-memory data structures, e.g. HashSet.
  // To implement it:
  // - open a scan cursor of the given table
  // - extract the attrName of every row and insert into a new temporary table
  // - close the previous cursor
  // - open another cursor on the temp table
  // - every time it getNext/getPrevious, check if it encounters the same attrVal with the previous one.
  Iterator project(String tableName, String attrName, boolean isDuplicateFree);

  Iterator project(Iterator iterator, String attrName, boolean isInputGettingLast);

  Set<Record> simpleProject(String tableName, String attrName, boolean isDuplicateFree);

  Set<Record> simpleProject(Iterator iterator, String attrName, boolean isInputGettingLast);


  Iterator join(Iterator outerIterator, Iterator innerIterator,
                ComparisonPredicate predicate, Set<String> attrNames);


  public StatusCode insert(String tableName, Record record, String[] primaryKeys);

  // dataSourceIterator should be the select iterator
  // if the data source iterator is null, update all records in the table
  public StatusCode update(String tableName, AssignmentExpression assignPredicate, Iterator dataSourceIterator);


  // iterator comes from the select
  // if the iterator is null, delete all records in the table
  public StatusCode delete(String tableName, Iterator iterator);

}

