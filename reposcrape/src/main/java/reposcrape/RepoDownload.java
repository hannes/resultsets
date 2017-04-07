package reposcrape;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
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

    public RetrievalTask(File infile) {
      this.inputfile = infile;
    }

    private void copyInputStreamToFile(InputStream in, File file) {
      try {
        OutputStream out = new FileOutputStream(file);
        byte[] buf = new byte[16 * 1024];
        int len;
        while ((len = in.read(buf)) > 0) {
          out.write(buf, 0, len);
        }
        out.close();
        in.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    private void payload(HttpClient httpClient, String url, File out) {
      log.info(url);
      HttpGet request = new HttpGet(url);

      try {

        File tempOut = File.createTempFile("reposcrape", ".zip");

        HttpResponse result = httpClient.execute(request);
        if (result.getStatusLine().getStatusCode() != 200) {
          throw new IOException(
              "HTTP error " + result.getStatusLine().getStatusCode() + " ("
                  + EntityUtils.toString(result.getEntity(), "UTF-8") + ")");
        }

        InputStream is = result.getEntity().getContent();
        copyInputStreamToFile(is, tempOut);
        Files.move(tempOut.toPath(), out.toPath());
      } catch (Exception e) {
        log.error(url + ":" + e.getMessage());
      }
    }

    public void run() {
      log.info(inputfile);
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
          File outputfile = Paths
              .get(outputDir,
                  linep[0] + "__" + reponame.replaceAll("/", "__") + ".zip")
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
        return name.matches("resultsets_\\d+");
      }
    })) {
      while (taskQueue.remainingCapacity() < 1) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          // ok
        }
      }

      ex.submit(new RetrievalTask(infile));
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
