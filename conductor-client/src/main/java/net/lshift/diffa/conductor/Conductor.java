package net.lshift.diffa.conductor;

public interface Conductor {

  void registerDriver(String space, String endpoint, DriverConfiguration configuration);

  /**
   * For want of a better word ....
   */
  void begin(String space, String endpoint);
}
