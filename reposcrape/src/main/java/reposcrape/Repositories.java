package reposcrape;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class Repositories {
  private String outputDir;
  private List<String> apiKeys;
  private int repoIdUpperBound;
  private int threads;
  private int reposPerChunk;

  private static Logger log = Logger.getLogger(Repositories.class);

  public Repositories(String outputDir, List<String> apiKeys,
      int repoIdUpperBound, int threads, int reposPerChunk) {
    this.outputDir = outputDir;
    this.apiKeys = apiKeys;
    this.repoIdUpperBound = repoIdUpperBound;
    this.threads = threads;
    this.reposPerChunk = reposPerChunk;
  }

  private class RetrievalTask implements Runnable {
    private int startid;
    private int endid;
    private File outputfile;
    private String apiKey;

    public RetrievalTask(String outputfile, String apiKey, int startid,
        int endid) {
      this.startid = startid;
      this.endid = endid;
      this.outputfile = new File(outputfile);
      this.apiKey = apiKey;
    }

    public void run() {
      if (outputfile.exists() && outputfile.length() > 0) {
        log.info("Skipping chunk, output file exists " + outputfile);
        return;
      }
      // actual payload
      String url = "https://api.github.com/repositories?access_token=" + apiKey
          + "&since=" + startid;
      CloseableHttpClient httpClient = HttpClientBuilder.create().build();
      FileOutputStream out = null;
      File outFile = null;
      boolean carry_on = true;

      try {
        outFile = File.createTempFile("reposcrape", "tsv");
        out = new FileOutputStream(outFile);
      } catch (Exception e1) {
        log.error(e1.getMessage());
        return;
      }

      while (carry_on) {
        try {
          HttpGet request = new HttpGet(url);
          request.addHeader("content-type", "application/json");
          HttpResponse result = httpClient.execute(request);
          if (result.getStatusLine().getStatusCode() != 200) {
            throw new IOException(
                "HTTP error " + result.getStatusLine().getStatusCode() + " ("
                    + EntityUtils.toString(result.getEntity(), "UTF-8") + ")");
          }
          String next = result.getHeaders("Link")[0].getElements()[0]
              .toString();
          url = next.substring(next.indexOf("<") + 1, next.indexOf(">"));
          log.debug(url);
          String json = EntityUtils.toString(result.getEntity(), "UTF-8");
          JsonElement jelement = new JsonParser().parse(json);
          JsonArray jarr = jelement.getAsJsonArray();
          if (jarr.size() < 1) {
            Files.move(outFile.toPath(), outputfile.toPath());
            log.info("Empty response: " + json);
            break;
          }
          for (int i = 0; i < jarr.size(); i++) {
            JsonObject jo = (JsonObject) jarr.get(i);

            if (jo.get("id").getAsInt() > endid) {
              Files.move(outFile.toPath(), outputfile.toPath());
              log.info(
                  "Success at " + jo.get("id").getAsInt() + " (" + endid + ")");
              carry_on = false;
              break;
            }

            out.write((jo.get("id").getAsString() + "\t"
                + jo.get("full_name").getAsString() + "\t"
                + jo.get("fork").getAsString() + "\n").getBytes());
          }

        } catch (Exception ex) {
          log.warn(ex.getMessage());
        }
      }
      try {
        out.close();
      } catch (IOException e) {
        // ok
      }

    }
  }

  public void retrieve() {
    BlockingQueue<Runnable> taskQueue = new LinkedBlockingDeque<Runnable>(1000);
    ExecutorService ex = new ThreadPoolExecutor(threads, threads,
        Integer.MAX_VALUE, TimeUnit.DAYS, taskQueue,
        new ThreadPoolExecutor.DiscardPolicy());

    for (int i = 0, startid = 0, endid = Math.min(reposPerChunk - 1,
        repoIdUpperBound); startid < repoIdUpperBound; i++, startid += reposPerChunk, endid = Math
            .min(endid + reposPerChunk, repoIdUpperBound)) {

      while (taskQueue.remainingCapacity() < 1) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          // ok
        }
      }
      String outfile = outputDir + File.separator + "repositories_"
          + String.format("%05d", i);
      log.info("#" + i + " [" + startid + "," + endid + "] > " + outfile);
      ex.submit(new RetrievalTask(outfile, apiKeys.get(i % apiKeys.size()),
          startid, endid));
    }

    ex.shutdown();
    try {
      ex.awaitTermination(Integer.MAX_VALUE, TimeUnit.DAYS);
    } catch (InterruptedException e) {
      // ok
    }
  }

}
