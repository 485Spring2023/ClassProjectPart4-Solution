package CSCI485ClassProject.models;

public class JoinCondition {
  public enum JoinType {
    NATURAL_JOIN,
    JOIN,
    SEMI_JOIN
  }

  /**
   * utilities for initializing a Join condition
   */
  public static JoinCondition CreateNatualJoinCondition(String attrName) {
    return new JoinCondition(JoinType.NATURAL_JOIN, attrName);
  }

  public static JoinCondition CreateJoinCondition(String attrName, Object attrVal, ComparisonOperator compOperator) {
    return new JoinCondition(JoinType.JOIN, attrName, attrVal, compOperator);
  }

  public static JoinCondition CreateSemiJoinCondition(String attrName, Object attrVal, ComparisonOperator compOperator) {
    return new JoinCondition(JoinCondition.JoinType.SEMI_JOIN, attrName, attrVal, compOperator);
  }

  private final JoinType joinType;

  private final String attrName;

  private Object attrVal;

  private ComparisonOperator compOperator;

  // initialize a NATURAL join condition
  public JoinCondition(JoinType joinType, String attrName) {
    this.joinType = joinType;
    this.attrName = attrName;
  }

  // initialize a JOIN/SEMI_JOIN condition
  public JoinCondition(JoinType joinType, String attrName, Object attrVal, ComparisonOperator compOperator) {
    this.joinType = joinType;
    this.attrName = attrName;
    this.attrVal = attrVal;
    this.compOperator = compOperator;
  }

  public JoinType getJoinType() {
    return joinType;
  }

  public String getAttrName() {
    return attrName;
  }

  public Object getAttrVal() {
    return attrVal;
  }

  public ComparisonOperator getCompOperator() {
    return compOperator;
  }
}