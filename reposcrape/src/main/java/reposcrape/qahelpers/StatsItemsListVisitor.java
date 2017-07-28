package reposcrape.qahelpers;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor;
import net.sf.jsqlparser.expression.operators.relational.MultiExpressionList;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SubSelect;

class StatsItemsListVisitor implements ItemsListVisitor {
  private SelectVisitor sv;
  private SomeExpressionVisitor ev; // TODO ?

  public StatsItemsListVisitor(SelectVisitor sv, SomeExpressionVisitor ev) {
    this.sv = sv;
    this.ev = ev;
  }

  public void visit(SubSelect subSelect) {
    ev.getParent().inc(ev.getPrefix(), "num_ops");
    ev.getParent().inc(ev.getPrefix(), "num_subquery");
    subSelect.getSelectBody().accept(sv);
  }

  public void visit(ExpressionList expressionList) {
    for (Expression e : expressionList.getExpressions()) {
      e.accept(ev);
    }
  }

  public void visit(MultiExpressionList multiExprList) {
    for (ExpressionList el : multiExprList.getExprList()) {
      el.accept(this);
    }
  }
}