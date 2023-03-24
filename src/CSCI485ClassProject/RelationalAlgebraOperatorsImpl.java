package CSCI485ClassProject;

import CSCI485ClassProject.models.AssignmentPredicate;
import CSCI485ClassProject.models.ComparisonPredicate;
import CSCI485ClassProject.models.Record;

import java.util.Set;

// your codes
public class RelationalAlgebraOperatorsImpl implements RelationalAlgebraOperators {
  @Override
  public Iterator select(String tableName, ComparisonPredicate predicate, boolean isUsingIndex) {
    return null;
  }

  @Override
  public Set<Record> simpleSelect(String tableName, ComparisonPredicate predicate, boolean isUsingIndex) {
    return null;
  }

  @Override
  public Iterator project(String tableName, String attrName, boolean isDuplicateFree) {
    return null;
  }

  @Override
  public Iterator project(Iterator iterator, String attrName, boolean isInputGettingLast) {
    return null;
  }

  @Override
  public Set<Record> simpleProject(String tableName, String attrName, boolean isDuplicateFree) {
    return null;
  }

  @Override
  public Set<Record> simpleProject(Iterator iterator, String attrName, boolean isInputGettingLast) {
    return null;
  }

  @Override
  public Iterator join(Iterator iterator1, Iterator iterator2, ComparisonPredicate predicate, String[] attrNames) {
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
