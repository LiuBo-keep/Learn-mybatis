package org.my.test.session;

import java.sql.Connection;

/**
 * @author aidan.liu
 * @version 1.0
 * @since 2026/1/9 16:07
 */
public enum TransactionIsolationLevel {
  NONE(Connection.TRANSACTION_NONE),

  READ_COMMITTED(Connection.TRANSACTION_READ_COMMITTED),

  READ_UNCOMMITTED(Connection.TRANSACTION_READ_UNCOMMITTED),

  REPEATABLE_READ(Connection.TRANSACTION_REPEATABLE_READ),

  SERIALIZABLE(Connection.TRANSACTION_SERIALIZABLE),

  SQL_SERVER_SNAPSHOT(0x1000);

  private final int level;

  TransactionIsolationLevel(int level) {
    this.level = level;
  }

  public int getLevel() {
    return level;
  }
}
