package CSCI485ClassProject.models;

public class ComparisonPredicate {
  private String subjectAttrName;

  ComparisonOperator operator;

  // either a specific value, or another attribute
  private Object operandValue = null;
  private String operandAttrName;

  public ComparisonPredicate(String subjectAttrName, ComparisonOperator operator, Object operandValue) {
    this.subjectAttrName = subjectAttrName;
    this.operator = operator;
    this.operandValue = operandValue;
  }

  public ComparisonPredicate(String subjectAttrName, ComparisonOperator operator, String operandAttrName) {
    this.subjectAttrName = subjectAttrName;
    this.operator = operator;
    this.operandAttrName = operandAttrName;
  }

  public String getSubjectAttrName() {
    return subjectAttrName;
  }

  public void setSubjectAttrName(String subjectAttrName) {
    this.subjectAttrName = subjectAttrName;
  }

  public ComparisonOperator getOperator() {
    return operator;
  }

  public void setOperator(ComparisonOperator operator) {
    this.operator = operator;
  }

  public Object getOperandValue() {
    return operandValue;
  }

  public void setOperandValue(Object operandValue) {
    this.operandValue = operandValue;
  }

  public String getOperandAttrName() {
    return operandAttrName;
  }

  public void setOperandAttrName(String operandAttrName) {
    this.operandAttrName = operandAttrName;
  }
}
