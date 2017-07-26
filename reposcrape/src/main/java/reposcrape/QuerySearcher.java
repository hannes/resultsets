package reposcrape;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.file.Paths;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.log4j.Logger;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;

public class QuerySearcher {
  private String outputDir;
  private String inputDir;
  private int threads;
  public int attempts;
  public int success;
  public int total;

  private static Logger log = Logger.getLogger(QuerySearcher.class);
  
  public QuerySearcher(String inputDir, String outputDir, int threads) {
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
    
    private String remove_unmatched_bracket(String in) {
      char[] chars = in.toCharArray();
      int open = 0;
      int end = in.length();
      for (int i = 0; i < chars.length; i++) {

        if (chars[i] == '(') {
          open++;
          continue;
        }
        if (chars[i] == ')') {
          if (open == 0) {
            end = i;
            break;
          }
          open--;
          continue;
        }
      }
      return in.substring(0, end);
    }
    
    private String remove_unmatched_quote(String in) {
      char[] chars = in.toCharArray();
      boolean open = false;
      int end = in.length();
      for (int i = 0; i < chars.length; i++) {

        if (chars[i] == '"') {
          open = !open;
          end = i;
        }
      }
      if (open) {
        return in.substring(0, end);
      }
      return in;
    }
    
    public void run() {
      ZipFile zipFile = null;
      File resultFile = null;
      File failFile = null;
   
      OutputStream os = null;
      try {
        resultFile = Paths
            .get(outputDir,
                inputfile.getName().replace(".zip", ".sql"))
            .toFile();
        
        failFile = Paths
            .get(outputDir,
                inputfile.getName().replace(".zip", ".fail"))
            .toFile();
        
        if (resultFile.exists() || failFile.exists()) {
          return;
        }
        
        if (inputfile.length() > 524288000) { // 500 MB max...
          return;
        }
        
        Pattern p = Pattern.compile("(SELECT\\s[^;]+\\sFROM\\s[^;]+)", Pattern.CASE_INSENSITIVE); 
        
        os = new FileOutputStream(resultFile);

        // check if the file has maven pom.xml in root dir
        zipFile = new ZipFile(inputfile);
        Enumeration<? extends ZipEntry> entries = zipFile.entries();
        while (entries.hasMoreElements()) {
          ZipEntry entry = entries.nextElement();
          // log.info(entry.getName());
          if (!entry.getName().matches(".*\\.java$")) {
            continue;
          }
          InputStream is = zipFile.getInputStream(entry);
          String fileData = streamToString(is);
          is.close();

          Matcher m = p.matcher(fileData);
          while (m.find()) {
            total++;
            log.debug(entry.getName());
            String cleaned_query = m.group(1).replaceAll("\n", " ").replaceAll("\\s+", " ").replaceAll("\"\\s*\\+\\s*\"", "").trim();
            cleaned_query = remove_unmatched_bracket(cleaned_query);
            cleaned_query = remove_unmatched_quote(cleaned_query);
            cleaned_query = cleaned_query.replaceAll("\"\\s*[+][^+]+[+]\\s*\"", " ? ");
            cleaned_query = cleaned_query.replaceAll("[\\\\]?[`\"']%[.a-z0-9]+[\\\\]?[`\"']", " ? ");
            cleaned_query = cleaned_query.replaceAll("%[.a-z0-9]+", " ? ");
            cleaned_query = cleaned_query.replaceAll(":\\w+", " ? ");
            
            try {
              Statement stmt = CCJSqlParserUtil.parse(cleaned_query);
              success++;
              os.write(cleaned_query.getBytes());
              os.write('\n');
            }
            catch (JSQLParserException e) {
             // System.out.println(cleaned_query);
             // System.out.println(e.getCause().getMessage());
            }      
          }
        }
        os.close();
        zipFile.close();
      } catch (Exception e) {
        log.info(inputfile + ": " + e.toString());
        
        try {
          if (resultFile != null) {
            resultFile.delete();
          }
        } catch (Exception e2) {}
        
        
        try {
          if (failFile != null) {
            failFile.createNewFile();
          }
        } catch (Exception e2) {}
        
        try {
          if (zipFile != null) {
            zipFile.close();
          }
        } catch (Exception e2) {}
        
        try {
          if (os != null) {
            os.close();
          }
        } catch (Exception e2) {}
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
    new QuerySearcher(res.getString("input"), res.getString("output"),
        res.getInt("threads")).retrieve();
    
  }

}