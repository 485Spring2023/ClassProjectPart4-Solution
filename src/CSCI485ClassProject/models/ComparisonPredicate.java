package CSCI485ClassProject.models;

import CSCI485ClassProject.StatusCode;

import static CSCI485ClassProject.StatusCode.PREDICATE_NOT_VALID;

public class ComparisonPredicate {

  // mode that indicates what type of the predicate is
  // 0 --- NONE
  // 1 --- e.g. Salary < 1500, Name == "Bob"
  // 2 --- e.g. Salary >= 1.5 * Age
  private int mode = 0;

  private String leftHandSideAttrName; // e.g. Salary == 1.1 * Age
  private AttributeType leftHandSideAttrType;

  ComparisonOperator operator; // in the example, it is ==

  // either a specific value, or another attribute
  private Object rightHandSideValue = null; // in the example, it is 1.1
  private AlgebraicOperator rightHandSideOperator; // in the example, it is *

  private String rightHandSideAttrName; // in the example, it is Age
  private AttributeType rightHandSideAttrType;



  public ComparisonPredicate() {}

  // e.g. Salary == 10000, Salary <= 5000
  public ComparisonPredicate(String leftHandSideAttrName, AttributeType leftHandSideAttrType, ComparisonOperator operator, Object rightHandSideValue) {
    mode = 1;
    this.leftHandSideAttrName = leftHandSideAttrName;
    this.leftHandSideAttrType = leftHandSideAttrType;
    this.operator = operator;
    this.rightHandSideValue = rightHandSideValue;
  }

  // e.g. Salary == 1.1 * Age
  public ComparisonPredicate(String leftHandSideAttrName, AttributeType leftHandSideAttrType, ComparisonOperator operator, String rightHandSideAttrName, AttributeType rightHandSideAttrType, Object rightHandSideValue, AlgebraicOperator rightHandSideOperator) {
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
    if (mode == 0) {
      return StatusCode.PREDICATE_VALID;
    } else if (mode == 1) {
      // e.g. Salary > 2000
      if (leftHandSideAttrType == AttributeType.NULL
          || (leftHandSideAttrType == AttributeType.INT && !(rightHandSideValue instanceof Integer) && !(rightHandSideValue instanceof Long))
          || (leftHandSideAttrType == AttributeType.DOUBLE && !(rightHandSideValue instanceof Double) && !(rightHandSideValue instanceof Float))
          || (leftHandSideAttrType == AttributeType.VARCHAR && !(rightHandSideValue instanceof String))) {
          return StatusCode.PREDICATE_NOT_VALID;
      }
    } else if (mode == 2) {
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


  public boolean isRecordQualified(Record record) {
    if (validate() != StatusCode.PREDICATE_VALID) {
      return false;
    }

    if (mode == 0) {
      return true;
    }

    // check if the leftHandSideAttr exists in the record
    if (record.getValueForGivenAttrName(leftHandSideAttrName) == null) {
      // leftHandSideAttr does not exist in the record, record is unqualified
      return false;
    }
    Object leftVal = record.getValueForGivenAttrName(leftHandSideAttrName);
    if (mode == 1) {
      // e.g. Salary >= 2000
      if (leftHandSideAttrType == AttributeType.INT) {
        return ComparisonPredicateUtils.compareTwoINT(leftVal, rightHandSideValue, operator);
      } else if (leftHandSideAttrType == AttributeType.DOUBLE) {
        return ComparisonPredicateUtils.compareTwoDOUBLE(leftVal, rightHandSideValue, operator);
      } else if (leftHandSideAttrType == AttributeType.VARCHAR) {
        return ComparisonPredicateUtils.compareTwoVARCHAR(leftVal, rightHandSideValue, operator);
      }
    } else {
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
