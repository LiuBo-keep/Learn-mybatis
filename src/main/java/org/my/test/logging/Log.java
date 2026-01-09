package org.my.test.logging;

/**
 * @author aidan.liu
 * @version 1.0
 * @since 2026/1/9 14:16
 */
public interface Log {
  boolean isDebugEnabled();

  boolean isTraceEnabled();

  void error(String s, Throwable e);

  void error(String s);

  void debug(String s);

  void trace(String s);

  void warn(String s);
}
