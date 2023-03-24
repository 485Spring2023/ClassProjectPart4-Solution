package CSCI485ClassProject;

import CSCI485ClassProject.models.AssignmentPredicate;
import CSCI485ClassProject.models.ComparisonOperator;
import CSCI485ClassProject.models.ComparisonPredicate;
import CSCI485ClassProject.models.AssignmentOperator;

public interface RelationalAlgebraOperators {

  Cursor select(String tableName, ComparisonPredicate predicate, boolean isGettingLast, boolean isUsingIndex);

  Cursor project(String tableName, String attrName, boolean isOutputGettingLast);

  // cursor may be the result of select/join/project operators
  Cursor project(Cursor cursor, boolean isInputGettingLast, String attrName);


  Cursor join(String table1Name, String table2Name,
              ComparisonPredicate predicate, boolean isOutputGettingLast);

  // cursor1 and cursor2 may be the result of select/join/project operators
  Cursor join(Cursor cursor1, boolean isGettingLast1, Cursor cursor2, boolean isGettingLast2,
              ComparisonPredicate predicate, boolean isOutputGettingLast);


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
