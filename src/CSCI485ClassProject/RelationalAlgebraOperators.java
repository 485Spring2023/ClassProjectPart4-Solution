package CSCI485ClassProject;

import CSCI485ClassProject.models.ComparisonOperator;
import CSCI485ClassProject.models.JoinCondition;
import CSCI485ClassProject.models.PredicateConnectorType;

public interface RelationalAlgebraOperators {

  /**
   * select operator. Retrieve records from a table and returns a cursor of the result records
   * @param tableName the target tableName, e.g. Employee
   * @param attrNames the attribute names in predicates, e.g. ["SSN", "Salary"]
   * @param attrValues the attribute values in predicates, e.g. [123, 15000]
   * @param operators the comparison operators between the attribute name and value, e.g. [>, <=]
   * @param predConn the connector between predicates, e.g. SSN>123 ∧ Salary<=1500
   * @param isGettingLast true if the result cursor is initialized by getLast
   * @param isUsingIndex indicates whether using index structure for each attribute, e.g. [true, false] means using index on SSN, not using index on Salary
   * @return the cursor that is used to iterate the result records
   */
  Cursor select(String tableName, String[] attrNames, Object[] attrValues, ComparisonOperator[] operators,
      PredicateConnectorType predConn, boolean isGettingLast, boolean[] isUsingIndex);

  /**
   * select operator. Retrieve records from another cursor and returns a cursor of the result records
   * @param cursor the cursor that points to a set of records
   * @param isInputGettingLast true if the input cursor is initialized by getLast
   * @param attrNames the attribute names in predicates, e.g. ["SSN", "Salary"]
   * @param attrValues the attribute values in predicates, e.g. [123, 15000]
   * @param operators the comparison operators between the attribute name and value, e.g. [>, <=]
   * @param predConn the connector between predicates, e.g. SSN>123 ∧ Salary<=1500
   * @param isOutputGettingLast true if the result cursor is going to be initialized by getLast
   * @param isUsingIndex indicates whether using index structure for each attribute, e.g. [true, false] means using index on SSN, not using index on Salary
   * @return the cursor that is used to iterate the result records
   */
  Cursor select(Cursor cursor, boolean isInputGettingLast, String[] attrNames, Object[] attrValues, ComparisonOperator[] operators,
                PredicateConnectorType predConn, boolean isOutputGettingLast, boolean[] isUsingIndex);

  /**
   * project operator. Returns a cursor that projects the certain attributes out from a given READ cursor.
   * @param cursor the input cursor that points to certain records.
   * @param isInputGettingLast true if the input cursor is initialized by getLast
   * @param attrNames the target attribute names to be projected from records.
   * @param isOutputGettingLast true if the output cursor is initialized by getLast.
   * @return the cursor that projects certain attributes of records.
   */
  Cursor project(Cursor cursor, boolean isInputGettingLast, String[] attrNames, boolean isOutputGettingLast);

  /**
   * join operator. Returns a cursor that joins two set of records(pointed with two cursors) with certain join condition
   * @param cursor1 first cursor that points to a set of records.
   * @param isGettingLast1 true if the cursor1 is initialized by getLast
   * @param cursor2 second cursor that points to a set of records.
   * @param isGettingLast2 true if the cursor1 is initialized by getLast
   * @param condition join condition. See class {JoinCondition} for detailed definitions.
   * @param isOutputGettingLast true if the output cursor is initialized by getLast
   * @return the cursor that points to the records after join operation.
   */
  Cursor join(Cursor cursor1, boolean isGettingLast1, Cursor cursor2, boolean isGettingLast2, JoinCondition condition, boolean isOutputGettingLast);


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
   * update operator. Updates existing records pointed by the cursor of a target table.
   *
   * @param tableName the target tableName
   * @param cursor the cursor that points to a set of records
   * @param isGettingLast true if the input cursor is initialized by getLast
   * @return StatusCode
   */
  StatusCode update(String tableName, Cursor cursor, boolean isGettingLast);

  /**
   * delete operator. Deletes existing records pointed by the cursor of a target table.
   * @param tableName the target tableName
   * @param cursor the cursor that points to a set of records
   * @param isGettingLast true if the input cursor is initialized by getLast
   * @return StatusCode
   */
  StatusCode delete(String tableName, Cursor cursor, boolean isGettingLast);
}
