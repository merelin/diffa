package net.lshift.diffa.adapter.scanning;

import javax.servlet.http.HttpServletRequest;

// TODO Unit test!!
public class SliceSizeParser {

  public final static int DEFAULT_SLICE_SIZE = 100;
  public final static String MAX_SLICE_SIZE_PARAMETER_NAME = "max-slice-size";

  private final HttpServletRequest req;

  public SliceSizeParser(HttpServletRequest r) {
    this.req = r;
  }

  public int getMaxSliceSize() {
    String slice = req.getParameter(MAX_SLICE_SIZE_PARAMETER_NAME);
    if (slice != null && slice.length() > 0) {
      return Integer.parseInt(slice);
    }
    else {
      return DEFAULT_SLICE_SIZE;
    }
  }
}
