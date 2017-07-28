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

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor;
import net.sf.jsqlparser.expression.operators.relational.MultiExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SubSelect;
import reposcrape.qahelpers.SelectAnalysisVisitor;

public class QueryAnalyzer {
  private String outputDir;
  private String inputDir;
  private int threads;
  public int attempts;
  public int success;
  public int total;

  static Logger log = Logger.getLogger(QueryAnalyzer.class);

  public QueryAnalyzer(String inputDir, String outputDir, int threads) {
    this.inputDir = inputDir;
    this.threads = threads;
    this.outputDir = outputDir;
  }

  
  // TODO: maxdepth

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

            os.write(line.replace((char) 0, ' ').getBytes("UTF-8"));
            os.write('\t');
            os.write(v.getLine().getBytes());
            os.write('\n');
          } catch (Exception e) {
            // should not happen

            System.out.println(line);
            e.printStackTrace();
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