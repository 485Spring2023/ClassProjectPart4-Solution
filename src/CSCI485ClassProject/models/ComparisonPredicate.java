package CSCI485ClassProject.models;

import CSCI485ClassProject.StatusCode;

import static CSCI485ClassProject.StatusCode.PREDICATE_NOT_VALID;

public class ComparisonPredicate {

  public enum Type {
    NONE, // meaning no predicate
    ONE_ATTR, // only one attribute is referenced, e.g. Salary < 1500, Name == "Bob"
    TWO_ATTR, // two attributes are referenced, e.g. Salary >= 1.5 * Age
  }

  private Type predicateType = Type.NONE;

  public Type getPredicateType() {
    return predicateType;
  }

  private String leftHandSideAttrName; // e.g. Salary == 1.1 * Age
  private AttributeType leftHandSideAttrType;

  ComparisonOperator operator; // in the example, it is ==

  // either a specific value, or another attribute
  private Object rightHandSideValue = null; // in the example, it is 1.1
  private AlgebraicOperator rightHandSideOperator; // in the example, it is *

  private String rightHandSideAttrName; // in the example, it is Age
  private AttributeType rightHandSideAttrType;

  public String getLeftHandSideAttrName() {
    return leftHandSideAttrName;
  }

  public void setLeftHandSideAttrName(String leftHandSideAttrName) {
    this.leftHandSideAttrName = leftHandSideAttrName;
  }

  public AttributeType getLeftHandSideAttrType() {
    return leftHandSideAttrType;
  }

  public void setLeftHandSideAttrType(AttributeType leftHandSideAttrType) {
    this.leftHandSideAttrType = leftHandSideAttrType;
  }

  public ComparisonOperator getOperator() {
    return operator;
  }

  public void setOperator(ComparisonOperator operator) {
    this.operator = operator;
  }

  public Object getRightHandSideValue() {
    return rightHandSideValue;
  }

  public void setRightHandSideValue(Object rightHandSideValue) {
    this.rightHandSideValue = rightHandSideValue;
  }

  public ComparisonPredicate() {}

  // e.g. Salary == 10000, Salary <= 5000
  public ComparisonPredicate(String leftHandSideAttrName, AttributeType leftHandSideAttrType, ComparisonOperator operator, Object rightHandSideValue) {
    predicateType = Type.ONE_ATTR;
    this.leftHandSideAttrName = leftHandSideAttrName;
    this.leftHandSideAttrType = leftHandSideAttrType;
    this.operator = operator;
    this.rightHandSideValue = rightHandSideValue;
  }

  // e.g. Salary == 1.1 * Age
  public ComparisonPredicate(String leftHandSideAttrName, AttributeType leftHandSideAttrType, ComparisonOperator operator, String rightHandSideAttrName, AttributeType rightHandSideAttrType, Object rightHandSideValue, AlgebraicOperator rightHandSideOperator) {
    predicateType = Type.TWO_ATTR;
    this.leftHandSideAttrName = leftHandSideAttrName;
    this.leftHandSideAttrType = leftHandSideAttrType;
    this.operator = operator;
    this.rightHandSideAttrName = rightHandSideAttrName;
    this.rightHandSideAttrType = rightHandSideAttrType;
    this.rightHandSideValue = rightHandSideValue;
    this.rightHandSideOperator = rightHandSideOperator;
  }

  // validate the predicate, return PREDICATE_VALID if the predicate is valid
  public StatusCode validate() {
    if (predicateType == Type.NONE) {
      return StatusCode.PREDICATE_VALID;
    } else if (predicateType == Type.ONE_ATTR) {
      // e.g. Salary > 2000
      if (leftHandSideAttrType == AttributeType.NULL
          || (leftHandSideAttrType == AttributeType.INT && !(rightHandSideValue instanceof Integer) && !(rightHandSideValue instanceof Long))
          || (leftHandSideAttrType == AttributeType.DOUBLE && !(rightHandSideValue instanceof Double) && !(rightHandSideValue instanceof Float))
          || (leftHandSideAttrType == AttributeType.VARCHAR && !(rightHandSideValue instanceof String))) {
          return StatusCode.PREDICATE_NOT_VALID;
      }
    } else if (predicateType == Type.TWO_ATTR) {
      // e.g. Salary >= 10 * Age
      if (leftHandSideAttrType == AttributeType.NULL || rightHandSideAttrType == AttributeType.NULL
          || (leftHandSideAttrType == AttributeType.VARCHAR || rightHandSideAttrType == AttributeType.VARCHAR)
          || (leftHandSideAttrType != rightHandSideAttrType)
          || (leftHandSideAttrType == AttributeType.INT && !(rightHandSideValue instanceof Integer) && !(rightHandSideValue instanceof Long)
          || (leftHandSideAttrType == AttributeType.DOUBLE && !(rightHandSideValue instanceof Double) && !(rightHandSideValue instanceof Float)))) {
        return PREDICATE_NOT_VALID;
      }
    }
    return StatusCode.PREDICATE_VALID;
  }

  // verify that two records are qualified
  public boolean isRecordQualified(Record rec1, Record rec2) {
    // TODO: finish this
    return false;
  }

  // verify that the record is qualified
  public boolean isRecordQualified(Record record) {
    if (record == null) {
      return false;
    }

    if (validate() != StatusCode.PREDICATE_VALID) {
      return false;
    }

    if (predicateType == Type.NONE) {
      return true;
    }

    // check if the leftHandSideAttr exists in the record
    if (record.getValueForGivenAttrName(leftHandSideAttrName) == null) {
      // leftHandSideAttr does not exist in the record, record is unqualified
      return false;
    }
    Object leftVal = record.getValueForGivenAttrName(leftHandSideAttrName);
    if (predicateType == Type.ONE_ATTR) {
      // e.g. Salary >= 2000
      if (leftHandSideAttrType == AttributeType.INT) {
        return ComparisonPredicateUtils.compareTwoINT(leftVal, rightHandSideValue, operator);
      } else if (leftHandSideAttrType == AttributeType.DOUBLE) {
        return ComparisonPredicateUtils.compareTwoDOUBLE(leftVal, rightHandSideValue, operator);
      } else if (leftHandSideAttrType == AttributeType.VARCHAR) {
        return ComparisonPredicateUtils.compareTwoVARCHAR(leftVal, rightHandSideValue, operator);
      }
    } else {
      // TWO_ATTR
      // check if the rightHandSideAttr exists in the record
      if (record.getValueForGivenAttrName(rightHandSideAttrName) == null) {
        // leftHandSideAttr does not exist in the record, record is unqualified
        return false;
      }
      Object rightVal = record.getValueForGivenAttrName(leftHandSideAttrName);
      if (leftHandSideAttrType == AttributeType.INT) {
        Object resultVal = AlgebraicOperatorUtils.computeTwoINT(leftVal, rightHandSideValue, rightHandSideOperator);
        return ComparisonPredicateUtils.compareTwoINT(leftVal, resultVal, operator);
      } else {
        Object resultVal = AlgebraicOperatorUtils.computeTwoDOUBLE(leftVal, rightHandSideValue, rightHandSideOperator);
        return ComparisonPredicateUtils.compareTwoDOUBLE(leftVal, resultVal, operator);
      }
    }
    return false;
  }

}
