package org.my.test.transaction.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.my.test.logging.Log;
import org.my.test.session.TransactionIsolationLevel;
import org.my.test.transaction.Transaction;
import org.my.test.transaction.TransactionException;

/**
 * 基于 JDBC {@link Connection} 的事务实现。
 *
 * <p>
 * 该类是 {@link Transaction} 接口的 JDBC 实现，用于直接通过 JDBC API
 * 管理事务的提交、回滚和连接生命周期。
 * </p>
 *
 * <p>
 * {@code JdbcTransaction} 既支持：
 * </p>
 * <ul>
 *   <li>基于 {@link DataSource} 延迟获取连接（MyBatis 默认方式）</li>
 *   <li>基于外部传入的 {@link Connection}（如 Spring 托管事务）</li>
 * </ul>
 *
 * <p>
 * 事务隔离级别与 autoCommit 行为在连接首次创建时进行配置，
 * 并在事务关闭时根据配置决定是否恢复 autoCommit 状态。
 * </p>
 *
 * <p>
 * 该实现刻意保持“轻事务模型”，不缓存状态、不感知 SQL 执行内容，
 * 仅根据 JDBC 规范判断是否需要提交或回滚。
 * </p>
 *
 * @author aidan.liu
 * @since 2026/1/9
 */
public class JdbcTransaction implements Transaction {

  /**
   * 日志实例。
   */
  private static final Log log = LogFactory.getLog(JdbcTransaction.class);

  /**
   * 当前事务使用的 JDBC 连接。
   *
   * <p>
   * 可能由 {@link DataSource} 延迟获取，
   * 也可能由外部直接注入。
   * </p>
   */
  protected Connection connection;

  /**
   * 数据源。
   *
   * <p>
   * 仅在使用 DataSource 构造事务时存在，
   * 用于在首次调用 {@link #getConnection()} 时获取连接。
   * </p>
   */
  protected DataSource dataSource;

  /**
   * 期望设置的事务隔离级别。
   *
   * <p>
   * 在连接创建后、事务开始前设置，
   * 若为 {@code null} 则保持数据库默认隔离级别。
   * </p>
   */
  protected TransactionIsolationLevel level;

  /**
   * 是否启用 JDBC autoCommit。
   *
   * <p>
   * {@code false} 表示显式事务控制（需要 commit / rollback），
   * {@code true} 表示每条 SQL 自动提交。
   * </p>
   */
  protected boolean autoCommit;

  /**
   * 是否在事务关闭时跳过 autoCommit 重置。
   *
   * <p>
   * 默认情况下，事务关闭前会尝试将 autoCommit 重置为 {@code true}，
   * 以避免部分数据库在关闭连接前强制要求提交或回滚。
   * </p>
   *
   * <p>
   * 在连接池（如 HikariCP、Druid）统一管理连接状态的场景下，
   * 可将该值设置为 {@code true} 以避免多余或有风险的 JDBC 调用。
   * </p>
   */
  protected boolean skipSetAutoCommitOnClose;

  /**
   * 使用数据源创建 JDBC 事务。
   *
   * <p>
   * 默认在事务关闭时会重置 autoCommit 状态。
   * </p>
   *
   * @param ds                数据源
   * @param desiredLevel      期望的事务隔离级别
   * @param desiredAutoCommit 是否启用 autoCommit
   */
  public JdbcTransaction(DataSource ds,
                         TransactionIsolationLevel desiredLevel,
                         boolean desiredAutoCommit) {
    this(ds, desiredLevel, desiredAutoCommit, false);
  }

  /**
   * 使用数据源创建 JDBC 事务，并指定是否在关闭时跳过 autoCommit 重置。
   *
   * @param ds                       数据源
   * @param desiredLevel             期望的事务隔离级别
   * @param desiredAutoCommit        是否启用 autoCommit
   * @param skipSetAutoCommitOnClose 是否跳过关闭时的 autoCommit 重置
   */
  public JdbcTransaction(DataSource ds,
                         TransactionIsolationLevel desiredLevel,
                         boolean desiredAutoCommit,
                         boolean skipSetAutoCommitOnClose) {
    dataSource = ds;
    level = desiredLevel;
    autoCommit = desiredAutoCommit;
    this.skipSetAutoCommitOnClose = skipSetAutoCommitOnClose;
  }

  /**
   * 使用已有 JDBC {@link Connection} 创建事务。
   *
   * <p>
   * 该构造方法通常用于外部事务管理场景，
   * 如 Spring 管理的事务。
   * </p>
   *
   * <p>
   * 在此模式下，连接的生命周期与 autoCommit 状态
   * 由外部系统负责管理。
   * </p>
   *
   * @param connection 已存在的 JDBC 连接
   */
  public JdbcTransaction(Connection connection) {
    this.connection = connection;
  }

  /**
   * 获取当前事务的 JDBC 连接。
   *
   * <p>
   * 若连接尚未创建，则通过 {@link DataSource} 获取并初始化连接：
   * </p>
   * <ul>
   *   <li>设置事务隔离级别</li>
   *   <li>配置 autoCommit 行为</li>
   * </ul>
   *
   * @return JDBC 连接
   * @throws SQLException 获取连接失败
   */
  @Override
  public Connection getConnection() throws SQLException {
    if (connection == null) {
      openConnection();
    }
    return connection;
  }

  /**
   * 提交事务。
   *
   * <p>
   * 仅在以下条件满足时才会执行提交：
   * </p>
   * <ul>
   *   <li>连接已创建</li>
   *   <li>autoCommit 为 {@code false}</li>
   * </ul>
   *
   * @throws SQLException 提交失败
   */
  @Override
  public void commit() throws SQLException {
    if (connection != null && !connection.getAutoCommit()) {
      if (log.isDebugEnabled()) {
        log.debug("Committing JDBC Connection [" + connection + "]");
      }
      connection.commit();
    }
  }

  /**
   * 回滚事务。
   *
   * <p>
   * 与 {@link #commit()} 类似，
   * 仅在非 autoCommit 模式下执行回滚。
   * </p>
   *
   * @throws SQLException 回滚失败
   */
  @Override
  public void rollback() throws SQLException {
    if (connection != null && !connection.getAutoCommit()) {
      if (log.isDebugEnabled()) {
        log.debug("Rolling back JDBC Connection [" + connection + "]");
      }
      connection.rollback();
    }
  }

  /**
   * 关闭事务并释放 JDBC 连接。
   *
   * <p>
   * 在关闭连接前，可能会尝试将 autoCommit 重置为 {@code true}，
   * 以兼容部分数据库对事务结束状态的要求。
   * </p>
   *
   * @throws SQLException 关闭连接失败
   */
  @Override
  public void close() throws SQLException {
    if (connection != null) {
      resetAutoCommit();
      if (log.isDebugEnabled()) {
        log.debug("Closing JDBC Connection [" + connection + "]");
      }
      connection.close();
    }
  }

  /**
   * 设置期望的 autoCommit 状态。
   *
   * <p>
   * 若当前连接的 autoCommit 状态与期望值不同，
   * 则尝试通过 JDBC API 进行切换。
   * </p>
   *
   * @param desiredAutoCommit 期望的 autoCommit 值
   */
  protected void setDesiredAutoCommit(boolean desiredAutoCommit) {
    try {
      if (connection.getAutoCommit() != desiredAutoCommit) {
        if (log.isDebugEnabled()) {
          log.debug("Setting autocommit to " + desiredAutoCommit
              + " on JDBC Connection [" + connection + "]");
        }
        connection.setAutoCommit(desiredAutoCommit);
      }
    } catch (SQLException e) {
      throw new TransactionException(
          "Error configuring AutoCommit. Your driver may not support "
              + "getAutoCommit() or setAutoCommit(). Requested setting: "
              + desiredAutoCommit + ". Cause: " + e,
          e);
    }
  }

  /**
   * 在关闭连接前重置 autoCommit 状态。
   *
   * <p>
   * 某些数据库在执行 SELECT 语句后也会隐式开启事务，
   * 并要求在关闭连接前执行 commit 或 rollback。
   * </p>
   *
   * <p>
   * 将 autoCommit 设为 {@code true} 可作为一种兼容性解决方案。
   * </p>
   */
  protected void resetAutoCommit() {
    try {
      if (!skipSetAutoCommitOnClose && !connection.getAutoCommit()) {
        if (log.isDebugEnabled()) {
          log.debug("Resetting autocommit to true on JDBC Connection [" + connection + "]");
        }
        connection.setAutoCommit(true);
      }
    } catch (SQLException e) {
      if (log.isDebugEnabled()) {
        log.debug("Error resetting autocommit before closing the connection. Cause: " + e);
      }
    }
  }

  /**
   * 打开并初始化 JDBC 连接。
   *
   * <p>
   * 初始化过程包括：
   * </p>
   * <ul>
   *   <li>从 {@link DataSource} 获取连接</li>
   *   <li>设置事务隔离级别</li>
   *   <li>配置 autoCommit 状态</li>
   * </ul>
   *
   * @throws SQLException 获取或初始化连接失败
   */
  protected void openConnection() throws SQLException {
    if (log.isDebugEnabled()) {
      log.debug("Opening JDBC Connection");
    }
    connection = dataSource.getConnection();
    if (level != null) {
      connection.setTransactionIsolation(level.getLevel());
    }
    setDesiredAutoCommit(autoCommit);
  }

  /**
   * 获取事务超时时间。
   *
   * <p>
   * JDBC 事务不直接支持超时控制，
   * 该方法默认返回 {@code null}。
   * </p>
   *
   * @return {@code null}
   */
  @Override
  public Integer getTimeout() throws SQLException {
    return null;
  }
}

