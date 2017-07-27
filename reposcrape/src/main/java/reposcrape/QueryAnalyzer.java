package reposcrape;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;

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

public class QueryAnalyzer {
  private String outputDir;
  private String inputDir;
  private int threads;
  public int attempts;
  public int success;
  public int total;

  private static Logger log = Logger.getLogger(QueryAnalyzer.class);

  public QueryAnalyzer(String inputDir, String outputDir, int threads) {
    this.inputDir = inputDir;
    this.threads = threads;
    this.outputDir = outputDir;
  }

  private class SelectAnalysisVisitor implements StatementVisitor {

    private class SomeExpressionVisitor implements ExpressionVisitor {
      
      private SelectVisitor sv = null;
      public SomeExpressionVisitor(SelectVisitor sv) {
        this.sv = sv;
      }

      public void visit(NullValue nullValue) {
        num_constants++;
      }

      public void visit(Function function) {
        num_ops++;
        function.accept(this);
      }

      public void visit(SignedExpression signedExpression) {
        num_ops++;
        num_math++;
        signedExpression.accept(this);
      }

      public void visit(JdbcParameter jdbcParameter) {
        num_vars++;
      }

      public void visit(JdbcNamedParameter jdbcNamedParameter) {
        num_vars++;
      }

      public void visit(DoubleValue doubleValue) {
        num_constants++;
      }

      public void visit(LongValue longValue) {
        num_constants++;
      }

      public void visit(HexValue hexValue) {
        num_constants++;
      }

      public void visit(DateValue dateValue) {
        num_constants++;
      }

      public void visit(TimeValue timeValue) {
        num_constants++;
      }

      public void visit(TimestampValue timestampValue) {
        num_constants++;
      }

      public void visit(Parenthesis parenthesis) {
        parenthesis.accept(this);
      }

      public void visit(StringValue stringValue) {
        num_constants++;
      }

      public void visit(Addition addition) {
        num_ops++;
        num_math++;
        addition.accept(this);
      }

      public void visit(Division division) {
        num_ops++;
        num_math++;
        division.accept(this);
      }

      public void visit(Multiplication multiplication) {
        num_ops++;
        num_math++;
        multiplication.accept(this);
      }

      public void visit(Subtraction subtraction) {
        num_ops++;
        num_math++;
        subtraction.accept(this);
      }

      public void visit(AndExpression andExpression) {
        num_ops++;
        num_bool++;
        andExpression.accept(this);
      }

      public void visit(OrExpression orExpression) {
        num_ops++;
        num_bool++;
        orExpression.accept(this);
      }

      public void visit(Between between) {
        num_ops++;
        num_comp++;
        between.accept(this);
      }

      public void visit(EqualsTo equalsTo) {
        num_ops++;
        num_comp++;
        equalsTo.accept(this);
      }

      public void visit(GreaterThan greaterThan) {
        num_ops++;
        num_comp++;
        greaterThan.accept(this);
      }

      public void visit(GreaterThanEquals greaterThanEquals) {
        num_ops++;
        num_comp++;
        greaterThanEquals.accept(this);
      }

      public void visit(InExpression inExpression) {
        num_set_ops++;
        num_ops++;
        inExpression.accept(this);
      }

      public void visit(IsNullExpression isNullExpression) {
        num_ops++;
        num_comp++;
        isNullExpression.accept(this);
      }

      public void visit(LikeExpression likeExpression) {
        num_ops++;
        num_comp++;
        likeExpression.accept(this);
      }

      public void visit(MinorThan minorThan) {
        num_ops++;
        num_comp++;
        minorThan.accept(this);
      }

      public void visit(MinorThanEquals minorThanEquals) {
        num_ops++;
        num_comp++;
        minorThanEquals.accept(this);
      }

      public void visit(NotEqualsTo notEqualsTo) {
        num_ops++;
        num_comp++;
        notEqualsTo.accept(this);
      }

      public void visit(Column tableColumn) {
        // nop
      }

      public void visit(SubSelect subSelect) {
        num_ops++;
        num_subquery++;
        subSelect.getSelectBody().accept(sv);
      }

      public void visit(CaseExpression caseExpression) {
        num_ops++;
        caseExpression.accept(this);
      }

      public void visit(WhenClause whenClause) {
        num_ops++;
        whenClause.accept(this);
      }

      public void visit(ExistsExpression existsExpression) {
        num_set_ops++;
        num_ops++;
        existsExpression.accept(this);
      }

      public void visit(AllComparisonExpression allComparisonExpression) {
        num_set_ops++;
        num_ops++;
        allComparisonExpression.accept(this);
      }

      public void visit(AnyComparisonExpression anyComparisonExpression) {
        num_set_ops++;
        num_ops++;
        anyComparisonExpression.accept(this);
      }

      public void visit(Concat concat) {
        num_ops++;
        concat.accept(this);
      }

      public void visit(Matches matches) {
        num_ops++;
        num_comp++;
        matches.accept(this);
      }

      public void visit(BitwiseAnd bitwiseAnd) {
        num_ops++;
        bitwiseAnd.accept(this);
      }

      public void visit(BitwiseOr bitwiseOr) {
        num_ops++;
        bitwiseOr.accept(this);
      }

      public void visit(BitwiseXor bitwiseXor) {
        num_ops++;
        bitwiseXor.accept(this);
      }

      public void visit(CastExpression cast) {
        num_ops++;
        cast.accept(this);
      }

      public void visit(Modulo modulo) {
        num_ops++;
        modulo.accept(this);
      }

      public void visit(AnalyticExpression aexpr) {
        num_ops++;
        num_windows++;
        aexpr.accept(this);
      }

      public void visit(WithinGroupExpression wgexpr) {
        num_ops++;
        wgexpr.accept(this);
      }

      public void visit(ExtractExpression eexpr) {
        num_ops++;
        eexpr.accept(this);
      }

      public void visit(IntervalExpression iexpr) {
        num_ops++;
        iexpr.accept(this);
      }

      public void visit(OracleHierarchicalExpression oexpr) {
        num_ops++;
      }

      public void visit(RegExpMatchOperator rexpr) {
        num_ops++;
      }

      public void visit(JsonExpression jsonExpr) {
        num_ops++;
      }

      public void visit(JsonOperator jsonExpr) {
        num_ops++;
      }

      public void visit(RegExpMySQLOperator regExpMySQLOperator) {
        num_ops++;
      }

      public void visit(UserVariable var) {
        num_vars++;
        num_ops++;
      }

      public void visit(NumericBind bind) {
        num_ops++;
      }

      public void visit(KeepExpression aexpr) {
        num_ops++;
      }

      public void visit(MySQLGroupConcat groupConcat) {
        num_ops++;
      }

      public void visit(RowConstructor rowConstructor) {
        // ?!
      }

      public void visit(OracleHint hint) {
        // hell no
      }

      public void visit(TimeKeyExpression timeKeyExpression) {
        num_ops++;
      }

      public void visit(DateTimeLiteralExpression literal) {
        num_ops++;
      }

      public void visit(NotExpression aThis) {
        num_ops++;
        aThis.getExpression().accept(this);
      }
    }

    public int num_set_ops = 0;
    public int num_vars = 0;
    public int num_windows = 0;
    public int num_constants = 0;
    public int num_math = 0;
    public int num_comp = 0;
    public int num_bool = 0;
    public int num_subquery = 0;
    public int num_joins = 0;
    public int num_groups = 0;
    public int num_order = 0;

    public int num_ops = 0;

    public void visit(Select select) {
      select.getSelectBody().accept(new SelectVisitor() {

        public void visit(PlainSelect ps) {
          final SelectVisitor sv = this;
          for (SelectItem si : ps.getSelectItems()) {
            si.accept(new SelectItemVisitor() {

              public void visit(AllColumns allColumns) {
                // this is a *, boring
              }

              public void visit(AllTableColumns allTableColumns) {
                // this is a a.*, boring
              }

              public void visit(SelectExpressionItem selectExpressionItem) {
                selectExpressionItem.getExpression()
                    .accept(new SomeExpressionVisitor(sv));
              }

            });
          }

          if (ps.getFromItem() != null) {
            ps.getFromItem().accept(new FromItemVisitor() {
              public void visit(Table tableName) {
                // nop
              }
  
              public void visit(SubSelect subSelect) {
                subSelect.accept(new SomeExpressionVisitor(sv));
              }
  
              public void visit(SubJoin subjoin) {
                num_ops++;
                num_joins++;
                subjoin.accept(this);
              }
  
              public void visit(LateralSubSelect lateralSubSelect) {
                throw new RuntimeException("lateralSubSelect");
              }
  
              public void visit(ValuesList valuesList) {
                throw new RuntimeException("valueslist");
              }
  
              public void visit(TableFunction tableFunction) {
                num_ops++;
                tableFunction.accept(this);
              }
  
            });
          }
          if (ps.getGroupByColumnReferences() != null) {
            num_groups += ps.getGroupByColumnReferences().size();
          }
          if (ps.getHaving() != null) {
            ps.getHaving().accept(new SomeExpressionVisitor(sv));
          }
          if (ps.getWhere() != null) {
            ps.getWhere().accept(new SomeExpressionVisitor(sv));
          }
          if (ps.getOrderByElements() != null) {
            num_order += ps.getOrderByElements().size();
          }
          if (ps.getLimit() != null) {
            num_ops++;
          }

          // TODO: deal with other parts of plainselect
        }

        public void visit(SetOperationList setOpList) {
          num_set_ops += setOpList.getOperations().size();
          for (SelectBody sb : setOpList.getSelects()) {
            num_ops++;
            sb.accept(this);
          }
        }

        public void visit(WithItem withItem) {
          num_ops++;
          withItem.getSelectBody().accept(this);
        }
      });
    }

    public void visit(Commit e) {
      log.warn("unexpected element " + e);
    }

    public void visit(Delete e) {
      log.warn("unexpected element " + e);
    }

    public void visit(Update e) {
      log.warn("unexpected element " + e);
    }

    public void visit(Insert e) {
      log.warn("unexpected element " + e);
    }

    public void visit(Replace e) {
      log.warn("unexpected element " + e);
    }

    public void visit(Drop e) {
      log.warn("unexpected element " + e);
    }

    public void visit(Truncate e) {
      log.warn("unexpected element " + e);
    }

    public void visit(CreateIndex e) {
      log.warn("unexpected element " + e);
    }

    public void visit(CreateTable e) {
      log.warn("unexpected element " + e);
    }

    public void visit(CreateView e) {
      log.warn("unexpected element " + e);
    }

    public void visit(AlterView e) {
      log.warn("unexpected element " + e);
    }

    public void visit(Alter e) {
      log.warn("unexpected element " + e);
    }

    public void visit(Statements e) {
      log.warn("unexpected element " + e);
    }

    public void visit(Execute e) {
      log.warn("unexpected element " + e);
    }

    public void visit(SetStatement e) {
      log.warn("unexpected element " + e);
    }

    public void visit(Merge e) {
      log.warn("unexpected element " + e);
    }

    public void visit(Upsert e) {
      log.warn("unexpected element " + e);
    }

  }

  private class FilterTask implements Runnable {
    private File inputfile;

    public FilterTask(File infile) {
      this.inputfile = infile;
    }

    public void run() {
      File resultFile = null;

      OutputStream os = null;
      try {
        resultFile = Paths
            .get(outputDir, inputfile.getName().replace(".sql", ".res"))
            .toFile();

        if (resultFile.exists() && resultFile.length() > 0) {
          return;
        }

        os = new FileOutputStream(resultFile);
        BufferedReader br = new BufferedReader(new FileReader(inputfile));
        String line = null;
        while ((line = br.readLine()) != null) {
          String[] parts = line.split("\t");
          if (parts.length < 5) {
            log.warn(line);
            continue;
          }
          String cleaned_query = parts[4];

          try {
            Statement stmt = CCJSqlParserUtil.parse(cleaned_query);

            SelectAnalysisVisitor v = new SelectAnalysisVisitor();

            stmt.accept(v);
            success++;

            os.write(line.getBytes());
            os.write('\t');
            os.write(v.num_ops);
            os.write('\n');
          } catch (JSQLParserException e) {
            // System.out.println(cleaned_query);
            // System.out.println(e.getCause().getMessage());
          }
        }

        br.close();
        os.close();
      } catch (Exception e) {
        log.info(inputfile + ": " + e);
        e.printStackTrace();

        try {
          if (resultFile != null) {
            resultFile.delete();
          }
        } catch (Exception e2) {
        }

        try {
          if (os != null) {
            os.close();
          }
        } catch (Exception e2) {
        }
      }

    }
  }

  public void retrieve() {
    BlockingQueue<Runnable> taskQueue = new LinkedBlockingDeque<Runnable>(
        10000);
    ExecutorService ex = new ThreadPoolExecutor(threads, threads,
        Integer.MAX_VALUE, TimeUnit.DAYS, taskQueue,
        new ThreadPoolExecutor.DiscardPolicy());

    File inputDirF = new File(inputDir);
    for (File infile : inputDirF.listFiles(new FilenameFilter() {
      public boolean accept(File dir, String name) {
        return name.matches(".*\\.sql");
      }
    })) {
      while (taskQueue.remainingCapacity() < 1) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          // ok
        }
      }

      ex.submit(new FilterTask(infile));
    }

    ex.shutdown();
    try {
      ex.awaitTermination(Integer.MAX_VALUE, TimeUnit.DAYS);
    } catch (InterruptedException e) {
      // ok
    }

    System.out.println("total=" + total);

    System.out.println("success=" + success);

  }

  public static void main(String[] args) throws JSAPException {
    JSAP jsap = new JSAP();

    jsap.registerParameter(new FlaggedOption("output").setShortFlag('o')
        .setLongFlag("output").setStringParser(JSAP.STRING_PARSER)
        .setRequired(true).setHelp("Output directory"));

    jsap.registerParameter(new FlaggedOption("input").setShortFlag('i')
        .setLongFlag("input").setStringParser(JSAP.STRING_PARSER)
        .setRequired(true).setHelp("Input directory"));

    jsap.registerParameter(new FlaggedOption("threads").setShortFlag('t')
        .setLongFlag("threads").setStringParser(JSAP.INTEGER_PARSER)
        .setRequired(true).setHelp("Threads to use"));

    JSAPResult res = jsap.parse(args);

    if (!res.success()) {
      @SuppressWarnings("rawtypes")
      Iterator errs = res.getErrorMessageIterator();
      while (errs.hasNext()) {
        System.err.println(errs.next());
      }
      System.err.println(
          "Usage: " + jsap.getUsage() + "\nParameters: " + jsap.getHelp());
      System.exit(-1);
    }
    new QueryAnalyzer(res.getString("input"), res.getString("output"),
        res.getInt("threads")).retrieve();

  }

}