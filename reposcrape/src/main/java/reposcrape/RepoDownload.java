package reposcrape;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;

public class RepoDownload {
  private String outputDir;
  private String inputDir;
  private int threads;

  private static Logger log = Logger.getLogger(RepoDownload.class);

  public RepoDownload(String inputDir, String outputDir, int threads) {
    this.outputDir = outputDir;
    this.inputDir = inputDir;
    this.threads = threads;
  }

  private class RetrievalTask implements Runnable {
    private File inputfile;
    private File outputfile;

    private static final int REQUEST_LENGTH = 6500;

    public RetrievalTask(File infile, File outfile) {
      this.inputfile = infile;
      this.outputfile = outfile;
    }

    private void payload(HttpClient httpClient, String url, File out) {
      HttpGet request = new HttpGet(url);
      try {
        HttpResponse result = httpClient.execute(request);
        if (result.getStatusLine().getStatusCode() != 200) {
          throw new IOException(
              "HTTP error " + result.getStatusLine().getStatusCode() + " ("
                  + EntityUtils.toString(result.getEntity(), "UTF-8") + ")");
        }

        InputStream is = result.getEntity().getContent();
        FileOutputStream fos = new FileOutputStream(out);
        int inByte;
        while ((inByte = is.read()) != -1)
          fos.write(inByte);
        is.close();
        fos.close();
      } catch (Exception e) {
        log.error(e.getMessage());
        try {
          Thread.sleep(1000 * 60 * 5);
        } catch (InterruptedException e1) {
          // TODO Auto-generated catch block
          e1.printStackTrace();
        }
        // try again?!
        payload(httpClient, url, out);
      }
    }

    public void run() {
      log.info(inputfile);
      if (outputfile.exists() && outputfile.length() > 0) {
        log.info("Skipping chunk, output file exists " + outputfile);
        return;
      }
      CloseableHttpClient httpClient = HttpClientBuilder.create().build();
      try {
        // oh, java...
        BufferedReader isr = new BufferedReader(
            new InputStreamReader(new FileInputStream(inputfile)));
        String line = null;
        while ((line = isr.readLine()) != null) {
          if (line.trim().equals("")) {
            continue;
          }
          String[] linep = line.split("\t");
          String reponame = linep[1];

          File outputfile = Paths.get(outputDir, reponame.replaceAll("/", "_"))
              .toFile();
          if (outputfile.exists() && outputfile.length() > 0) {
            continue;
          }
          payload(httpClient,
              "https://github.com/" + reponame + "/archive/master.zip",
              outputfile);
        }
        isr.close();
      } catch (IOException e) {
        log.warn(e.getMessage());
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
        return name.matches("repositories_\\d+");
      }
    })) {
      while (taskQueue.remainingCapacity() < 1) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          // ok
        }
      }

      File outfile = new File(outputDir + File.separator
          + infile.getName().replace("repositories", "resultsets"));

      log.info(outfile);
      ex.submit(new RetrievalTask(infile, outfile));
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
    new RepoDownload(res.getString("input"), res.getString("output"),
        res.getInt("threads")).retrieve();
  }

}
