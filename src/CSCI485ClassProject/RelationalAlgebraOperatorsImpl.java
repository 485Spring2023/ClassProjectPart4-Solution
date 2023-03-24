package CSCI485ClassProject;

import CSCI485ClassProject.models.AssignmentPredicate;
import CSCI485ClassProject.models.ComparisonPredicate;

// your codes
public class RelationalAlgebraOperatorsImpl implements RelationalAlgebraOperators {

  @Override
  public Cursor select(String tableName, ComparisonPredicate predicate, boolean isGettingLast, boolean isUsingIndex) {
    return null;
  }

  @Override
  public Cursor project(String tableName, String attrName, boolean isOutputGettingLast) {
    return null;
  }

  @Override
  public Cursor project(Cursor cursor, boolean isInputGettingLast, String attrName) {
    return null;
  }

  @Override
  public Cursor join(String table1Name, String table2Name, ComparisonPredicate predicate, boolean isOutputGettingLast) {
    return null;
  }

  @Override
  public Cursor join(Cursor cursor1, boolean isGettingLast1, Cursor cursor2, boolean isGettingLast2, ComparisonPredicate predicate, boolean isOutputGettingLast) {
    return null;
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
