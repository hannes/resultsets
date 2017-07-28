package reposcrape.qahelpers;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Commit;
import net.sf.jsqlparser.statement.SetStatement;
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
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.LateralSubSelect;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
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
import reposcrape.QueryAnalyzer;

public class SelectAnalysisVisitor extends StatVisitor
    implements StatementVisitor {
  
  private SelectAnalysisVisitor sav;
  
  public SelectAnalysisVisitor() {
    sav = this;
  }

  
  private void handleJoin(Join j, SelectVisitor sv, FromItemVisitor fiv) {
    if (j != null) {
      inc("from", "num_ops");
      inc("from", "num_joins");
      
      if (j.isCross()) {
        inc("from", "num_join_cross");
      }
      if(j.isFull()) {
        inc("from", "num_join_full");
      }
      if (j.isInner()) {
        inc("from", "num_join_inner");
      }
      if(j.isLeft()) {
        inc("from", "num_join_left");
      }
      if (j.isNatural()) {
        inc("from", "num_join_natural");
      }
      if (j.isOuter()) {
        inc("from", "num_join_outer");
      }
      if (j.isRight()) {
        inc("from", "num_join_right");
      }
      if (j.isSemi()) {
        inc("from", "num_join_semi");
      }
      if (j.isSimple()) {
        inc("from", "num_join_simple");
      }
      if (j.getOnExpression() != null) {
        inc("from", "num_ops");
        j.getOnExpression()
            .accept(new SomeExpressionVisitor(sav, sv, "from"));
      }
      if (j.getUsingColumns() != null) {
        inc("from", "num_ops");
        for (Column uc : j.getUsingColumns()) {
          uc.accept(new SomeExpressionVisitor(sav, sv, "from"));
        }
      }
      j.getRightItem().accept(fiv);
    }
    
  }
  
  public void visit(Select select) {
    select.getSelectBody().accept(new SelectVisitor() {
      public void visit(PlainSelect ps) {
        final SelectVisitor sv = this;
        for (SelectItem si : ps.getSelectItems()) {
          si.accept(new SelectItemVisitor() {

            public void visit(AllColumns allColumns) {
              inc("select", "num_stars");
            }

            public void visit(AllTableColumns allTableColumns) {
              inc("select", "num_stars");
            }

            public void visit(SelectExpressionItem selectExpressionItem) {
              selectExpressionItem.getExpression()
                  .accept(new SomeExpressionVisitor(sav, sv, "select"));
            }

          });
        }
        
        FromItemVisitor fiv = new FromItemVisitor() {
          public void visit(Table tableName) {
            inc("from", "num_tables");
          }

          public void visit(SubSelect subSelect) {
            inc("from", "num_ops");
            inc("from", "num_subquery");
            subSelect.accept(new SomeExpressionVisitor(sav, sv, "from"));
          }

          public void visit(SubJoin subjoin) {
            subjoin.getLeft().accept(this);
            final FromItemVisitor fiv = this;
            Join j = subjoin.getJoin();
            handleJoin(j, sv, fiv);
           }

          public void visit(LateralSubSelect lateralSubSelect) {
            throw new RuntimeException("lateralSubSelect");
          }

          public void visit(ValuesList valuesList) {
            valuesList.getMultiExpressionList()
                .accept(new StatsItemsListVisitor(sv,
                    new SomeExpressionVisitor(sav, sv, "from")));
          }

          public void visit(TableFunction tableFunction) {
            inc("from", "num_ops");
            tableFunction.getFunction()
                .accept(new SomeExpressionVisitor(sav, sv, "from"));
          }

        };
        
        if (ps.getJoins() != null) {
          for (Join j : ps.getJoins()) {
            handleJoin(j, sv, fiv);
          }
        }
        
        if (ps.getFromItem() != null) {
          ps.getFromItem().accept(fiv);
        }
        if (ps.getGroupByColumnReferences() != null) {
          inc("group", "num_groups", ps.getGroupByColumnReferences().size());
        }
        if (ps.getHaving() != null) {
          ps.getHaving().accept(new SomeExpressionVisitor(sav, sv, "having"));
        }
        if (ps.getWhere() != null) {
          ps.getWhere().accept(new SomeExpressionVisitor(sav, sv, "where"));
        }
        if (ps.getOrderByElements() != null) {
          inc("order", "num_order", ps.getOrderByElements().size());
        }
        if (ps.getLimit() != null) {
          inc("limit", "num_ops");
        }
      }

      public void visit(SetOperationList setOpList) {
        inc("set", "num_set_ops", setOpList.getOperations().size());
        for (SelectBody sb : setOpList.getSelects()) {
          inc("set", "num_ops");
          sb.accept(this);
        }
      }

      public void visit(WithItem withItem) {
        inc("with", "num_ops");
        withItem.getSelectBody().accept(this);
      }
    });
  }

  public void visit(Commit e) {
    throw new RuntimeException("unexpected element " + e);
  }

  public void visit(Delete e) {
    throw new RuntimeException("unexpected element " + e);
  }

  public void visit(Update e) {
    throw new RuntimeException("unexpected element " + e);
  }

  public void visit(Insert e) {
    throw new RuntimeException("unexpected element " + e);
  }

  public void visit(Replace e) {
    throw new RuntimeException("unexpected element " + e);
  }

  public void visit(Drop e) {
    throw new RuntimeException("unexpected element " + e);
  }

  public void visit(Truncate e) {
    throw new RuntimeException("unexpected element " + e);
  }

  public void visit(CreateIndex e) {
    throw new RuntimeException("unexpected element " + e);
  }

  public void visit(CreateTable e) {
    throw new RuntimeException("unexpected element " + e);
  }

  public void visit(CreateView e) {
    throw new RuntimeException("unexpected element " + e);
  }

  public void visit(AlterView e) {
    throw new RuntimeException("unexpected element " + e);
  }

  public void visit(Alter e) {
    throw new RuntimeException("unexpected element " + e);
  }

  public void visit(Statements e) {
    throw new RuntimeException("unexpected element " + e);
  }

  public void visit(Execute e) {
    throw new RuntimeException("unexpected element " + e);
  }

  public void visit(SetStatement e) {
    throw new RuntimeException("unexpected element " + e);
  }

  public void visit(Merge e) {
    throw new RuntimeException("unexpected element " + e);
  }

  public void visit(Upsert e) {
    throw new RuntimeException("unexpected element " + e);
  }

}