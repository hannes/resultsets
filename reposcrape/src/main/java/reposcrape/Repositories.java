package reposcrape;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
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
  private String apiKey;
  private int repoIdUpperBound;
  private int threads;

  private static final int REPOS_PER_CHUNK = 1000000;

  private static Logger log = Logger.getLogger(Repositories.class);

  public Repositories(String outputDir, String apiKey, int repoIdUpperBound,
      int threads) {
    this.outputDir = outputDir;
    this.apiKey = apiKey;
    this.repoIdUpperBound = repoIdUpperBound;
    this.threads = threads;
  }

  private class RetrievalTask implements Runnable {
    private int startid;
    private int endid;
    private File outputfile;

    public RetrievalTask(String outputfile, int startid, int endid) {
      this.startid = startid;
      this.endid = endid;
      System.out.println(outputfile);
      this.outputfile = new File(outputfile);
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
      boolean carry_on = true;

      try {
        out = new FileOutputStream(outputfile);
      } catch (FileNotFoundException e1) {
        log.error(e1.getMessage());
        return;
      }

      while (carry_on) {
        try {
          HttpGet request = new HttpGet(url);
          request.addHeader("content-type", "application/json");
          HttpResponse result = httpClient.execute(request);

          String next = result.getHeaders("Link")[0].getElements()[0]
              .toString();
          url = next.substring(next.indexOf("<") + 1, next.indexOf(">"));

          String json = EntityUtils.toString(result.getEntity(), "UTF-8");
          JsonElement jelement = new JsonParser().parse(json);
          JsonArray jarr = jelement.getAsJsonArray();
          if (jarr.size() < 1) {
            break;
          }
          for (int i = 0; i < jarr.size(); i++) {
            JsonObject jo = (JsonObject) jarr.get(i);

            if (jo.get("id").getAsInt() > endid) {
              carry_on = false;
              break;
            }

            out.write((jo.get("id").getAsString() + "\t"
                + jo.get("full_name").getAsString() + "\t"
                + jo.get("fork").getAsString() + "\n").getBytes());
          }

        } catch (IOException ex) {
          log.warn(ex.getMessage());
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
          }
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
    BlockingQueue<Runnable> taskQueue = new LinkedBlockingDeque<Runnable>(100);
    ExecutorService ex = new ThreadPoolExecutor(threads, threads,
        Integer.MAX_VALUE, TimeUnit.DAYS, taskQueue,
        new ThreadPoolExecutor.DiscardPolicy());

    for (int i = 0, startid = 0, endid = Math.min(REPOS_PER_CHUNK - 1,
        repoIdUpperBound); startid < repoIdUpperBound; i++, startid += REPOS_PER_CHUNK, endid = Math
            .min(endid + REPOS_PER_CHUNK, repoIdUpperBound)) {
      System.out.println(startid + " " + endid);

      while (taskQueue.remainingCapacity() < 1) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          // ok
        }
      }
      ex.submit(new RetrievalTask(outputDir + File.separator + "repositories_"
          + String.format("%05d", i), startid, endid));
    }

    ex.shutdown();
    try {
      ex.awaitTermination(Integer.MAX_VALUE, TimeUnit.DAYS);
    } catch (InterruptedException e) {
      // ok
    }
  }

}
