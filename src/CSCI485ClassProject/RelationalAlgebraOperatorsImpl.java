package CSCI485ClassProject;

import CSCI485ClassProject.models.ComparisonOperator;
import CSCI485ClassProject.models.JoinCondition;
import CSCI485ClassProject.models.PredicateConnectorType;

// your codes
public class RelationalAlgebraOperatorsImpl implements RelationalAlgebraOperators {
  @Override
  public Cursor select(String tableName, String[] attrNames, Object[] attrValues, ComparisonOperator[] operators, PredicateConnectorType predConn, boolean isGettingLast, boolean[] isUsingIndex) {
    return null;
  }

  @Override
  public Cursor select(Cursor cursor, boolean isInputGettingLast, String[] attrNames, Object[] attrValues, ComparisonOperator[] operators, PredicateConnectorType predConn, boolean isOutputGettingLast, boolean[] isUsingIndex) {
    return null;
  }

  @Override
  public Cursor project(Cursor cursor, boolean isGettingLast, String[] attrNames) {
    return null;
  }

  @Override
  public Cursor join(Cursor cursor1, boolean isGettingLast1, Cursor cursor2, boolean isGettingLast2, JoinCondition condition) {
    return null;
  }

  @Override
  public StatusCode insert(String tableName, Cursor cursor, boolean isGettingLast) {
    return null;
  }

  @Override
  public StatusCode update(String tableName, Cursor cursor, boolean isGettingLast) {
    return null;
  }

  @Override
  public StatusCode delete(String tableName, Cursor cursor, boolean isGettingLast) {
    return null;
  }
}
