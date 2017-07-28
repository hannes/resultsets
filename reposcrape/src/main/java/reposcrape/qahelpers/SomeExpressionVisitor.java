package reposcrape.qahelpers;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.DateTimeLiteralExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.ExtractExpression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.HexValue;
import net.sf.jsqlparser.expression.IntervalExpression;
import net.sf.jsqlparser.expression.JdbcNamedParameter;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.JsonExpression;
import net.sf.jsqlparser.expression.KeepExpression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.MySQLGroupConcat;
import net.sf.jsqlparser.expression.NotExpression;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.NumericBind;
import net.sf.jsqlparser.expression.OracleHierarchicalExpression;
import net.sf.jsqlparser.expression.OracleHint;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.RowConstructor;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeKeyExpression;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.UserVariable;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.WindowElement;
import net.sf.jsqlparser.expression.WithinGroupExpression;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Modulo;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.JsonOperator;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.expression.operators.relational.RegExpMatchOperator;
import net.sf.jsqlparser.expression.operators.relational.RegExpMySQLOperator;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.OrderByVisitor;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SubSelect;

class SomeExpressionVisitor
    implements ExpressionVisitor {

  private SelectVisitor sv = null;
  private String prefix = "";
  private StatVisitor p = null;

  public SomeExpressionVisitor(StatVisitor parent, SelectVisitor sv,
      String prefix) {
    this.sv = sv;
    this.prefix = prefix;
    this.p = parent;
  }
  
  public StatVisitor getParent() {
    return p;
  }
  
  public String getPrefix() {
    return prefix;
  }
  
  public void visit(NullValue nullValue) {
    p.inc(prefix, "num_constants");
  }

  public void visit(Function function) {
    p.inc(prefix, "num_ops");
    if (function.getParameters() != null) {
      function.getParameters().accept(new StatsItemsListVisitor(sv, this));
    }
  }

  public void visit(SignedExpression signedExpression) {
    p.inc(prefix, "num_ops");
    p.inc(prefix, "num_math");
    signedExpression.getExpression().accept(this);
  }

  public void visit(JdbcParameter jdbcParameter) {
    p.inc(prefix, "num_vars");
  }

  public void visit(JdbcNamedParameter jdbcNamedParameter) {
    p.inc(prefix, "num_vars");
  }

  public void visit(DoubleValue doubleValue) {
    p.inc(prefix, "num_constants");
  }

  public void visit(LongValue longValue) {
    p.inc(prefix, "num_constants");
  }

  public void visit(HexValue hexValue) {
    p.inc(prefix, "num_constants");
  }

  public void visit(DateValue dateValue) {
    p.inc(prefix, "num_constants");
  }

  public void visit(TimeValue timeValue) {
    p.inc(prefix, "num_constants");
  }

  public void visit(TimestampValue timestampValue) {
    p.inc(prefix, "num_constants");
  }

  public void visit(Parenthesis parenthesis) {
    parenthesis.getExpression().accept(this);
  }

  public void visit(StringValue stringValue) {
    p.inc(prefix, "num_constants");
  }

  public void visit(Addition op) {
    p.inc(prefix, "num_ops");
    p.inc(prefix, "num_math");
    op.getLeftExpression().accept(this);
    op.getRightExpression().accept(this);
  }

  public void visit(Division op) {
    p.inc(prefix, "num_ops");
    p.inc(prefix, "num_math");
    op.getLeftExpression().accept(this);
    op.getRightExpression().accept(this);
  }

  public void visit(Multiplication op) {
    p.inc(prefix, "num_ops");
    p.inc(prefix, "num_math");
    op.getLeftExpression().accept(this);
    op.getRightExpression().accept(this);
  }

  public void visit(Subtraction op) {
    p.inc(prefix, "num_ops");
    p.inc(prefix, "num_math");
    op.getLeftExpression().accept(this);
    op.getRightExpression().accept(this);
  }

  public void visit(AndExpression op) {
    p.inc(prefix, "num_ops");
    p.inc(prefix, "num_bool");
    op.getLeftExpression().accept(this);
    op.getRightExpression().accept(this);
  }

  public void visit(OrExpression op) {
    p.inc(prefix, "num_ops");
    p.inc(prefix, "num_bool");
    op.getLeftExpression().accept(this);
    op.getRightExpression().accept(this);
  }

  public void visit(Between between) {
    p.inc(prefix, "num_ops");
    p.inc(prefix, "num_comp");
    between.getLeftExpression().accept(this);
    between.getBetweenExpressionStart().accept(this);
    between.getBetweenExpressionEnd().accept(this);
  }

  public void visit(EqualsTo op) {
    p.inc(prefix, "num_ops");
    p.inc(prefix, "num_comp");
    op.getLeftExpression().accept(this);
    op.getRightExpression().accept(this);
  }

  public void visit(GreaterThan op) {
    p.inc(prefix, "num_ops");
    p.inc(prefix, "num_comp");
    op.getLeftExpression().accept(this);
    op.getRightExpression().accept(this);
  }

  public void visit(GreaterThanEquals op) {
    p.inc(prefix, "num_ops");
    p.inc(prefix, "num_comp");
    op.getLeftExpression().accept(this);
    op.getRightExpression().accept(this);
  }

  public void visit(InExpression op) {
    p.inc(prefix, "num_set_ops");
    p.inc(prefix, "num_ops");
    if (op.getLeftExpression() != null) {
      op.getLeftExpression().accept(this);
    }
    op.getRightItemsList().accept(new StatsItemsListVisitor(sv, this));
  }

  public void visit(IsNullExpression isNullExpression) {
    p.inc(prefix, "num_ops");
    p.inc(prefix, "num_comp");
    isNullExpression.getLeftExpression().accept(this);
  }

  public void visit(LikeExpression op) {
    p.inc(prefix, "num_ops");
    p.inc(prefix, "num_comp");
    op.getLeftExpression().accept(this);
    op.getRightExpression().accept(this);
  }

  public void visit(MinorThan op) {
    p.inc(prefix, "num_ops");
    p.inc(prefix, "num_comp");
    op.getLeftExpression().accept(this);
    op.getRightExpression().accept(this);
  }

  public void visit(MinorThanEquals op) {
    p.inc(prefix, "num_ops");
    p.inc(prefix, "num_comp");
    op.getLeftExpression().accept(this);
    op.getRightExpression().accept(this);
  }

  public void visit(NotEqualsTo op) {
    p.inc(prefix, "num_ops");
    p.inc(prefix, "num_comp");
    op.getLeftExpression().accept(this);
    op.getRightExpression().accept(this);
  }

  public void visit(Column tableColumn) {
    p.inc(prefix, "num_cols");
  }

  public void visit(SubSelect subSelect) {
    p.inc(prefix, "num_ops");
    p.inc(prefix, "num_subquery");
    subSelect.getSelectBody().accept(sv);
  }

  public void visit(CaseExpression caseExpression) {
    p.inc(prefix, "num_ops");
    for (Expression e : caseExpression.getWhenClauses()) {
      e.accept(this);
    }
    if (caseExpression.getSwitchExpression() != null) {
      caseExpression.getSwitchExpression().accept(this);
    }
    if (caseExpression.getElseExpression() != null) {
      caseExpression.getElseExpression().accept(this);
    }
  }

  public void visit(WhenClause whenClause) {
    p.inc(prefix, "num_ops");
    whenClause.getWhenExpression().accept(this);
    whenClause.getThenExpression().accept(this);
  }

  public void visit(ExistsExpression existsExpression) {
    p.inc(prefix, "num_set_ops");
    p.inc(prefix, "num_ops");
    existsExpression.getRightExpression().accept(this);
  }

  public void visit(AllComparisonExpression allComparisonExpression) {
    p.inc(prefix, "num_set_ops");
    p.inc(prefix, "num_ops");
    allComparisonExpression.getSubSelect().getSelectBody().accept(sv);
  }

  public void visit(AnyComparisonExpression anyComparisonExpression) {
    p.inc(prefix, "num_set_ops");
    p.inc(prefix, "num_ops");
    anyComparisonExpression.getSubSelect().getSelectBody().accept(sv);
  }

  public void visit(Concat op) {
    p.inc(prefix, "num_ops");
    op.getLeftExpression().accept(this);
    op.getRightExpression().accept(this);
  }

  public void visit(Matches op) {
    p.inc(prefix, "num_ops");
    p.inc(prefix, "num_comp");
    op.getLeftExpression().accept(this);
    op.getRightExpression().accept(this);
  }

  public void visit(BitwiseAnd op) {
    p.inc(prefix, "num_ops");
    op.getLeftExpression().accept(this);
    op.getRightExpression().accept(this);
  }

  public void visit(BitwiseOr op) {
    p.inc(prefix, "num_ops");
    op.getLeftExpression().accept(this);
    op.getRightExpression().accept(this);
  }

  public void visit(BitwiseXor op) {
    p.inc(prefix, "num_ops");
    op.getLeftExpression().accept(this);
    op.getRightExpression().accept(this);
  }

  public void visit(CastExpression cast) {
    p.inc(prefix, "num_ops");
    cast.getLeftExpression().accept(this);
  }

  public void visit(Modulo op) {
    p.inc(prefix, "num_ops");
    op.getLeftExpression().accept(this);
    op.getRightExpression().accept(this);
  }

  public void visit(AnalyticExpression aexpr) {
    p.inc(prefix, "num_ops");
    p.inc(prefix, "num_windows");
    if (aexpr.getDefaultValue() != null) {
      aexpr.getDefaultValue().accept(this);
    }
    if (aexpr.getExpression() != null) {
      aexpr.getExpression().accept(this);
    }
    if (aexpr.getPartitionExpressionList() != null) {
      aexpr.getPartitionExpressionList()
          .accept(new StatsItemsListVisitor(sv, this));
    }
    WindowElement we = aexpr.getWindowElement();

    if (we != null) {
      if (we.getOffset() != null) {
        if (we.getOffset().getExpression() != null) {
          we.getOffset().getExpression().accept(this);
        }
      }
      if (we.getRange() != null) {
        if (we.getRange().getStart().getExpression() != null) {
          we.getRange().getStart().getExpression().accept(this);
        }
        if (we.getRange().getEnd().getExpression() != null) {
          we.getRange().getEnd().getExpression().accept(this);
        }
      }
    }
    final ExpressionVisitor ev = this;
    if (aexpr.getOrderByElements() != null) {
      for (OrderByElement obe : aexpr.getOrderByElements()) {
        obe.accept(new OrderByVisitor() {
          public void visit(OrderByElement orderBy) {
            orderBy.getExpression().accept(ev);
          }
        });
      }
    }
  }

  public void visit(WithinGroupExpression wgexpr) {
    p.inc(prefix, "num_ops");
    wgexpr.getExprList().accept(new StatsItemsListVisitor(sv, this));
  }

  public void visit(ExtractExpression eexpr) {
    p.inc(prefix, "num_ops");
    eexpr.getExpression().accept(this);
  }

  public void visit(IntervalExpression iexpr) {
    p.inc(prefix, "num_ops");

  }

  public void visit(OracleHierarchicalExpression oexpr) {
    p.inc(prefix, "num_ops");
  }

  public void visit(RegExpMatchOperator rexpr) {
    p.inc(prefix, "num_ops");
  }

  public void visit(JsonExpression jsonExpr) {
    p.inc(prefix, "num_ops");
  }

  public void visit(JsonOperator jsonExpr) {
    p.inc(prefix, "num_ops");
  }

  public void visit(RegExpMySQLOperator regExpMySQLOperator) {
    p.inc(prefix, "num_ops");
  }

  public void visit(UserVariable var) {
    p.inc(prefix, "num_vars");
    p.inc(prefix, "num_ops");
  }

  public void visit(NumericBind bind) {
    p.inc(prefix, "num_ops");
  }

  public void visit(KeepExpression aexpr) {
    p.inc(prefix, "num_ops");
  }

  public void visit(MySQLGroupConcat groupConcat) {
    p.inc(prefix, "num_ops");
  }

  public void visit(RowConstructor rowConstructor) {
    // ?!
  }

  public void visit(OracleHint hint) {
    // hell no
  }

  public void visit(TimeKeyExpression timeKeyExpression) {
    p.inc(prefix, "num_ops");
  }

  public void visit(DateTimeLiteralExpression literal) {
    p.inc(prefix, "num_ops");
  }

  public void visit(NotExpression aThis) {
    p.inc(prefix, "num_ops");
    aThis.getExpression().accept(this);
  }
}