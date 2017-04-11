package reposcrape;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Path;
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

public class RepoBuilder {
  private String outputDir;
  private String inputDir;
  private int threads;
  public int attempts;
  public int success;
  public int total;

  private static Logger log = Logger.getLogger(RepoBuilder.class);

  public RepoBuilder(String inputDir, String outputDir, int threads) {
    this.inputDir = inputDir;
    this.threads = threads;
    this.outputDir = outputDir;
  }

  private class FilterTask implements Runnable {
    private File inputfile;

    public FilterTask(File infile) {
      this.inputfile = infile;
    }

    private String streamToString(InputStream is) {
      ByteArrayOutputStream result = new ByteArrayOutputStream();
      byte[] buffer = new byte[1024];
      int length;
      try {
        while ((length = is.read(buffer)) != -1) {
          result.write(buffer, 0, length);
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
      try {
        return result.toString("UTF-8");
      } catch (UnsupportedEncodingException e) {
        e.printStackTrace();
      }
      return "";
    }

    public void run() {
      log.debug(inputfile);
      boolean isMaven = false;
      Runtime rt = Runtime.getRuntime();
      Path t = null;
      Process pr = null;
      total++;
      if (total % 10 == 0) {
        log.info("Total: " + total + ", Attempts: " + attempts + ", Success: "
            + success);
      }
      try {
        if (inputfile.length() > 524288000) { // 500 MB max...
          return;
        }

        // check if the file has maven pom.xml in root dir
        ZipFile zipFile = new ZipFile(inputfile);
        Enumeration<? extends ZipEntry> entries = zipFile.entries();
        while (entries.hasMoreElements()) {
          ZipEntry entry = entries.nextElement();
          // log.info(entry.getName());
          if (entry.getName().matches("[^/]+/pom.xml")) {
            isMaven = true;
            break;
          }
        }
        // TODO: look for test cases here, too?
        zipFile.close();
        if (!isMaven) {
          return;
        }
        attempts++;

        log.debug(inputfile);

        // unzip into temp folder
        t = Files.createTempDirectory("repobuilder-tempdir");
        pr = rt.exec("unzip -q " + inputfile + " -d " + t);
        if (pr.waitFor() != 0) {
          throw new RuntimeException(
              "unzip failed " + streamToString(pr.getErrorStream()) + " "
                  + streamToString(pr.getInputStream()));
        }
        pr.destroy();
        File reporoot = null;
        for (File f : t.toFile().listFiles()) {
          if (f.isDirectory() && f.getName().contains("master")) {
            reporoot = f;
            break;
          }
        }
        if (reporoot == null) {
          throw new RuntimeException("could not find repo root");
        }

        // try running mvn install, use chroot to contain possible fallout
        pr = rt.exec(
            "fakechroot chroot " + reporoot
                + " mvn --no-snapshot-updates -Dmaven.repo.local=/export/scratch1/home/hannes/resultsets/reposcrape/mavenrepo -Dmaven.javadoc.skip=true -Dmaven.test.skip=true -Djava.net.preferIPv4Stack=true --batch-mode install ",
            new String[] {
                "FAKECHROOT_EXCLUDE_PATH=/bin:/usr:/etc:/proc:/export/scratch1/home/hannes/resultsets/reposcrape/mavenrepo" });
        if (pr.waitFor() != 0) {
          throw new RuntimeException(
              "build failed " + streamToString(pr.getErrorStream()) + " "
                  + streamToString(pr.getInputStream()));
        }
        pr.destroy();
        success++;
        log.debug("success on " + inputfile);

      } catch (Exception e) {
        log.debug(inputfile + ": " + e.getMessage());
        if (t != null) {
          try {
            // clean up
            pr = rt.exec("rm -rf  " + t);
            pr.waitFor();
            pr.destroy();
          } catch (Exception e1) {
            e.printStackTrace();
          }
        }
        if (pr != null) {
          pr.destroy();
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
    new RepoBuilder(res.getString("input"), res.getString("output"),
        res.getInt("threads")).retrieve();
  }

}