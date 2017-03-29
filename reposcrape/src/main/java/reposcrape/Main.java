package reposcrape;

import java.util.Iterator;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;

public class Main {

  public static void main(String[] args) throws JSAPException {
    JSAP jsap = new JSAP();

    jsap.registerParameter(new FlaggedOption("output").setShortFlag('o')
        .setLongFlag("output").setStringParser(JSAP.STRING_PARSER)
        .setRequired(true).setHelp("Output directory"));

    jsap.registerParameter(new FlaggedOption("apikey").setShortFlag('a')
        .setLongFlag("apikey").setStringParser(JSAP.STRING_PARSER)
        .setRequired(true).setHelp("Github API key"));

    jsap.registerParameter(new FlaggedOption("upperbound").setShortFlag('u')
        .setLongFlag("upperbound").setStringParser(JSAP.INTEGER_PARSER)
        .setRequired(true).setHelp("Github repository ID upper bound"));

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
    new Repositories(res.getString("output"), res.getString("apikey"),
        res.getInt("upperbound"), res.getInt("threads")).retrieve();
  }
}