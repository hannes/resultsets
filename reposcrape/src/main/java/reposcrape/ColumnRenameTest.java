package reposcrape;

import com.martiansoftware.jsap.JSAPException;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.DateTimeLiteralExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
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
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Commit;
import net.sf.jsqlparser.statement.SetStatement;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitor;
import net.sf.jsqlparser.statement.Statements;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.create.index.CreateIndex;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.view.AlterView;
import net.sf.jsqlparser.statement.create.view.CreateView;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.execute.Execute;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.merge.Merge;
import net.sf.jsqlparser.statement.replace.Replace;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.LateralSubSelect;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.TableFunction;
import net.sf.jsqlparser.statement.select.ValuesList;
import net.sf.jsqlparser.statement.select.WithItem;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.statement.upsert.Upsert;
import reposcrape.qahelpers.SelectAnalysisVisitor;

public class ColumnRenameTest {


  public static void main(String[] args) throws JSAPException, JSQLParserException {
   
    Statement stmt = CCJSqlParserUtil.parse("SELECT A FROM B");
    SelectAnalysisVisitor v = new SelectAnalysisVisitor();

    stmt.accept(new StatementVisitor() {

      public void visit(Commit commit) {
        // TODO Auto-generated method stub
        
      }

      public void visit(Delete delete) {
        // TODO Auto-generated method stub
        
      }

      public void visit(Update update) {
        // TODO Auto-generated method stub
        
      }

      public void visit(Insert insert) {
        // TODO Auto-generated method stub
        
      }

      public void visit(Replace replace) {
        // TODO Auto-generated method stub
        
      }

      public void visit(Drop drop) {
        // TODO Auto-generated method stub
        
      }

      public void visit(Truncate truncate) {
        // TODO Auto-generated method stub
        
      }

      public void visit(CreateIndex createIndex) {
        // TODO Auto-generated method stub
        
      }

      public void visit(CreateTable createTable) {
        // TODO Auto-generated method stub
        
      }

      public void visit(CreateView createView) {
        // TODO Auto-generated method stub
        
      }

      public void visit(AlterView alterView) {
        // TODO Auto-generated method stub
        
      }

      public void visit(Alter alter) {
        // TODO Auto-generated method stub
        
      }

      public void visit(Statements stmts) {
        // TODO Auto-generated method stub
        
      }

      public void visit(Execute execute) {
        // TODO Auto-generated method stub
        
      }

      public void visit(SetStatement set) {
        // TODO Auto-generated method stub
        
      }

      public void visit(Merge merge) {
        // TODO Auto-generated method stub
        
      }

      public void visit(Select select) {
        select.getSelectBody().accept(new SelectVisitor() {

          public void visit(PlainSelect ps) {
            
            ps.getFromItem().accept(new FromItemVisitor() {

              public void visit(Table tableName) {
                tableName.setName("FDSA");
                
              }

              public void visit(SubSelect subSelect) {
                // TODO Auto-generated method stub
                
              }

              public void visit(SubJoin subjoin) {
                // TODO Auto-generated method stub
                
              }

              public void visit(LateralSubSelect lateralSubSelect) {
                // TODO Auto-generated method stub
                
              }

              public void visit(ValuesList valuesList) {
                // TODO Auto-generated method stub
                
              }

              public void visit(TableFunction tableFunction) {
                // TODO Auto-generated method stub
                
              }
              
            });
            for (SelectItem si : ps.getSelectItems()) {
              si.accept(new SelectItemVisitor() {

                public void visit(AllColumns allColumns) {
                }

                public void visit(AllTableColumns allTableColumns) {
                }

                public void visit(SelectExpressionItem sei) {
                  sei.getExpression().accept(new ExpressionVisitor() {

                    public void visit(NullValue nullValue) {
                      // TODO Auto-generated method stub
                      
                    }

                    public void visit(Function function) {
                      // TODO Auto-generated method stub
                      
                    }

                    public void visit(SignedExpression signedExpression) {
                      // TODO Auto-generated method stub
                      
                    }

                    public void visit(JdbcParameter jdbcParameter) {
                      // TODO Auto-generated method stub
                      
                    }

                    public void visit(JdbcNamedParameter jdbcNamedParameter) {
                      // TODO Auto-generated method stub
                      
                    }

                    public void visit(DoubleValue doubleValue) {
                      // TODO Auto-generated method stub
                      
                    }

                    public void visit(LongValue longValue) {
                      // TODO Auto-generated method stub
                      
                    }

                    public void visit(HexValue hexValue) {
                      // TODO Auto-generated method stub
                      
                    }

                    public void visit(DateValue dateValue) {
                      
                      throw new RuntimeException("I don't like date values");
                      
                    }

                    public void visit(TimeValue timeValue) {
                      // TODO Auto-generated method stub
                      
                    }

                    public void visit(TimestampValue timestampValue) {
                      // TODO Auto-generated method stub
                      
                    }

                    public void visit(Parenthesis parenthesis) {
                      // TODO Auto-generated method stub
                      
                    }

                    public void visit(StringValue stringValue) {
                      // TODO Auto-generated method stub
                      
                    }

                    public void visit(Addition addition) {
                      // TODO Auto-generated method stub
                      
                    }

                    public void visit(Division division) {
                      // TODO Auto-generated method stub
                      
                    }

                    public void visit(Multiplication multiplication) {
                      // TODO Auto-generated method stub
                      
                    }

                    public void visit(Subtraction subtraction) {
                      // TODO Auto-generated method stub
                      
                    }

                    public void visit(AndExpression andExpression) {
                      // TODO Auto-generated method stub
                      
                    }

                    public void visit(OrExpression orExpression) {
                      // TODO Auto-generated method stub
                      
                    }

                    public void visit(Between between) {
                      // TODO Auto-generated method stub
                      
                    }

                    public void visit(EqualsTo equalsTo) {
                      // TODO Auto-generated method stub
                      
                    }

                    public void visit(GreaterThan greaterThan) {
                      // TODO Auto-generated method stub
                      
                    }

                    public void visit(GreaterThanEquals greaterThanEquals) {
                      // TODO Auto-generated method stub
                      
                    }

                    public void visit(InExpression inExpression) {
                      // TODO Auto-generated method stub
                      
                    }

                    public void visit(IsNullExpression isNullExpression) {
                      // TODO Auto-generated method stub
                      
                    }

                    public void visit(LikeExpression likeExpression) {
                      // TODO Auto-generated method stub
                      
                    }

                    public void visit(MinorThan minorThan) {
                      // TODO Auto-generated method stub
                      
                    }

                    public void visit(MinorThanEquals minorThanEquals) {
                      // TODO Auto-generated method stub
                      
                    }

                    public void visit(NotEqualsTo notEqualsTo) {
                      // TODO Auto-generated method stub
                      
                    }

                    public void visit(Column tableColumn) {
                     tableColumn.setColumnName("ASDF");
                      
                    }

                    public void visit(SubSelect subSelect) {
                      // TODO Auto-generated method stub
                      
                    }

                    public void visit(CaseExpression caseExpression) {
                      // TODO Auto-generated method stub
                      
                    }

                    public void visit(WhenClause whenClause) {
                      // TODO Auto-generated method stub
                      
                    }

                    public void visit(ExistsExpression existsExpression) {
                      // TODO Auto-generated method stub
                      
                    }

                    public void visit(
                        AllComparisonExpression allComparisonExpression) {
                      // TODO Auto-generated method stub
                      
                    }

                    public void visit(
                        AnyComparisonExpression anyComparisonExpression) {
                      // TODO Auto-generated method stub
                      
                    }

                    public void visit(Concat concat) {
                      // TODO Auto-generated method stub
                      
                    }

                    public void visit(Matches matches) {
                      // TODO Auto-generated method stub
                      
                    }

                    public void visit(BitwiseAnd bitwiseAnd) {
                      // TODO Auto-generated method stub
                      
                    }

                    public void visit(BitwiseOr bitwiseOr) {
                      // TODO Auto-generated method stub
                      
                    }

                    public void visit(BitwiseXor bitwiseXor) {
                      // TODO Auto-generated method stub
                      
                    }

                    public void visit(CastExpression cast) {
                      // TODO Auto-generated method stub
                      
                    }

                    public void visit(Modulo modulo) {
                      // TODO Auto-generated method stub
                      
                    }

                    public void visit(AnalyticExpression aexpr) {
                      // TODO Auto-generated method stub
                      
                    }

                    public void visit(WithinGroupExpression wgexpr) {
                      // TODO Auto-generated method stub
                      
                    }

                    public void visit(ExtractExpression eexpr) {
                      // TODO Auto-generated method stub
                      
                    }

                    public void visit(IntervalExpression iexpr) {
                      // TODO Auto-generated method stub
                      
                    }

                    public void visit(OracleHierarchicalExpression oexpr) {
                      // TODO Auto-generated method stub
                      
                    }

                    public void visit(RegExpMatchOperator rexpr) {
                      // TODO Auto-generated method stub
                      
                    }

                    public void visit(JsonExpression jsonExpr) {
                      // TODO Auto-generated method stub
                      
                    }

                    public void visit(JsonOperator jsonExpr) {
                      // TODO Auto-generated method stub
                      
                    }

                    public void visit(RegExpMySQLOperator regExpMySQLOperator) {
                      // TODO Auto-generated method stub
                      
                    }

                    public void visit(UserVariable var) {
                      // TODO Auto-generated method stub
                      
                    }

                    public void visit(NumericBind bind) {
                      // TODO Auto-generated method stub
                      
                    }

                    public void visit(KeepExpression aexpr) {
                      // TODO Auto-generated method stub
                      
                    }

                    public void visit(MySQLGroupConcat groupConcat) {
                      // TODO Auto-generated method stub
                      
                    }

                    public void visit(RowConstructor rowConstructor) {
                      // TODO Auto-generated method stub
                      
                    }

                    public void visit(OracleHint hint) {
                      // TODO Auto-generated method stub
                      
                    }

                    public void visit(TimeKeyExpression timeKeyExpression) {
                      // TODO Auto-generated method stub
                      
                    }

                    public void visit(DateTimeLiteralExpression literal) {
                      // TODO Auto-generated method stub
                      
                    }

                    public void visit(NotExpression aThis) {
                      // TODO Auto-generated method stub
                      
                    }
                    
                  });
                }
                
              });
              
             
            }
            
          }

          public void visit(SetOperationList setOpList) {
            // TODO Auto-generated method stub
            
          }

          public void visit(WithItem withItem) {
            // TODO Auto-generated method stub
            
          }

          
        });
        
      }

      public void visit(Upsert upsert) {
        // TODO Auto-generated method stub
        
      }
      
    });
    
    System.out.println(stmt.toString());
    
  }

}