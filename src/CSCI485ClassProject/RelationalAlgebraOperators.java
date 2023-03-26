package CSCI485ClassProject;

import CSCI485ClassProject.models.AssignmentPredicate;
import CSCI485ClassProject.models.ComparisonPredicate;
import CSCI485ClassProject.models.Record;

import java.util.Set;

public interface RelationalAlgebraOperators {
  Iterator select(String tableName, ComparisonPredicate predicate, boolean isUsingIndex);

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


  /**
   * insert operator. Inserts new records pointed by the cursor to a target table.
   *
   * If the given tableName does not exist, the table will be created.
   *
   * @param tableName the target tableName
   * @param cursor the cursor that points to a set of records
   * @param isGettingLast true if the input cursor is initialized by getLast
   * @return StatusCode
   */
  StatusCode insert(String tableName, Cursor cursor, boolean isGettingLast);

  /**
   * update operator.
   *
   * @param tableName the target tableName
   * @return StatusCode
   */
  StatusCode update(String tableName, AssignmentPredicate assignPredicate, ComparisonPredicate compPredicate);

  /**
   * delete operator. Deletes existing records pointed by the cursor of a target table.
   * @param tableName the target tableName
   * @return StatusCode
   */
  StatusCode delete(String tableName, ComparisonPredicate comparisonPredicate);
}
