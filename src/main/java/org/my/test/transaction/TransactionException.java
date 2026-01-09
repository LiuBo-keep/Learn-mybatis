package org.my.test.transaction;

import org.my.test.exceptions.PersistenceException;

/**
 * @author aidan.liu
 * @version 1.0
 * @since 2026/1/9 16:06
 */
public class TransactionException extends PersistenceException {

  private static final long serialVersionUID = -433589569461084605L;

  public TransactionException() {
  }

  public TransactionException(String message) {
    super(message);
  }

  public TransactionException(String message, Throwable cause) {
    super(message, cause);
  }

  public TransactionException(Throwable cause) {
    super(cause);
  }

}
