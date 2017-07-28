package reposcrape.qahelpers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

abstract class StatVisitor {
  private HashMap<String, Integer> stats = new HashMap<String, Integer>();

  protected void inc(String prefix, String key, int n) {
    String hkey = prefix + "_" + key;
    if (!stats.containsKey(hkey)) {
      stats.put(hkey, new Integer(0));
    }
    stats.put(hkey, stats.get(hkey) + n);
    if (prefix != "_overall") {
      inc("_overall", key, n);
    }
  }

  protected void inc(String prefix, String key) {
    inc(prefix, key, 1);
  }

  public String getLine() {
    String ret = "";

    List<String> keys = new ArrayList<String>(stats.keySet());
    Collections.sort(keys);
    for (String k : keys) {
      ret += k + "=" + stats.get(k) + ",";
    }

    return ret;
  }
}