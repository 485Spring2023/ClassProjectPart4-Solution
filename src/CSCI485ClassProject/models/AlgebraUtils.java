package CSCI485ClassProject.models;

public class AlgebraUtils {
  public static double computeTwoDOUBLE(Object val1, Object val2, AlgebraicOperator operator) {
    double v1 = (Double) val1;
    double v2 = (Double) val2;

    if (operator == AlgebraicOperator.MINUS) {
      return v1 / v2;
    } else if (operator == AlgebraicOperator.PRODUCT) {
      return v1 * v2;
    } else if (operator == AlgebraicOperator.PLUS) {
      return v1 + v2;
    } else {
      return v1 - v2;
    }
  }

  public static long computeTwoINT(Object val1, Object val2, AlgebraicOperator operator) {
    long v1;
    if (val1 instanceof Integer) {
      v1 = new Long((Integer) val1);
    } else {
      v1 = (long) val1;
    }

    long v2;
    if (val2 instanceof Integer) {
      v2 = new Long((Integer) val2);
    } else {
      v2 = (long) val2;
    }
    if (operator == AlgebraicOperator.MINUS) {
      return v1 / v2;
    } else if (operator == AlgebraicOperator.PRODUCT) {
      return v1 * v2;
    } else if (operator == AlgebraicOperator.PLUS) {
      return v1 + v2;
    } else {
      return v1 - v2;
    }
  }

}
