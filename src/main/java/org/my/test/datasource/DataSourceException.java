package org.my.test.datasource;

import org.my.test.exceptions.PersistenceException;

/**
 * @author aidan.liu
 * @version 1.0
 * @since 2026/1/9 13:52
 */
public class DataSourceException extends PersistenceException {

  private static final long serialVersionUID = -5251396250407091334L;

  public DataSourceException() {
  }

  public DataSourceException(String message) {
    super(message);
  }

  public DataSourceException(String message, Throwable cause) {
    super(message, cause);
  }

  public DataSourceException(Throwable cause) {
    super(cause);
  }

}
