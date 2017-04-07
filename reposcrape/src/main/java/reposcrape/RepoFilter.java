package reposcrape;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.log4j.Logger;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;

public class RepoFilter {
  private String outputDir;
  private String inputDir;
  private int threads;

  private int maven = 0;
  private int ivy = 0;
  private int ant = 0;
  private int total = 0;

  private static Logger log = Logger.getLogger(RepoFilter.class);

  public RepoFilter(String inputDir, String outputDir, int threads) {
    this.outputDir = outputDir;
    this.inputDir = inputDir;
    this.threads = threads;

  }

  private class FilterTask implements Runnable {
    private File inputfile;

    public FilterTask(File infile) {
      this.inputfile = infile;
    }

    public void run() {
      log.info(inputfile);
      total++;
      try {
        ZipFile zipFile = new ZipFile(inputfile);
        Enumeration<? extends ZipEntry> entries = zipFile.entries();
        while (entries.hasMoreElements()) {
          ZipEntry entry = entries.nextElement();
        //  log.info(entry.getName());
          if (entry.getName().endsWith("pom.xml")) {
            maven++;
          }if (entry.getName().endsWith("build.xml")) {
            ant++;
          }
          // InputStream stream = zipFile.getInputStream(entry);
        }
        zipFile.close();
      } catch (IOException e) {
        log.warn(inputfile + ": " + e.getMessage());
        e.printStackTrace();
      }

    }
  }

  public void retrieve() {
    BlockingQueue<Runnable> taskQueue = new LinkedBlockingDeque<Runnable>(1000);
    ExecutorService ex = new ThreadPoolExecutor(threads, threads,
        Integer.MAX_VALUE, TimeUnit.DAYS, taskQueue,
        new ThreadPoolExecutor.DiscardPolicy());

    File inputDirF = new File(inputDir);
    for (File infile : inputDirF.listFiles(new FilenameFilter() {
      public boolean accept(File dir, String name) {
        return name.matches(".*\\.zip");
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
      log.info("Total: " + total + ", Maven: " + maven + ", Ivy:" + ivy
          + ", Ant: " + ant);
    } catch (InterruptedException e) {
      // ok
    }
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
        .setRequired(true).setHelp("Threads to use (probably 1)"));

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
    new RepoFilter(res.getString("input"), res.getString("output"),
        res.getInt("threads")).retrieve();
  }

}
