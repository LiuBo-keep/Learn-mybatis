package org.my.test.transaction.managed;

import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.my.test.logging.Log;
import org.my.test.session.TransactionIsolationLevel;
import org.my.test.transaction.Transaction;

/**
 * 受外部事务管理器控制的事务实现。
 *
 * <p>
 * {@code ManagedTransaction} 表示事务的提交、回滚以及生命周期
 * 由外部系统（如 Spring 事务管理器或 JTA 容器）统一控制，
 * MyBatis 仅负责获取和暴露 {@link Connection}。
 * </p>
 *
 * <p>
 * 在该实现中：
 * </p>
 * <ul>
 *   <li>{@link #commit()} 与 {@link #rollback()} 为<strong>空操作</strong></li>
 *   <li>是否关闭 {@link Connection} 由 {@code closeConnection} 决定</li>
 * </ul>
 *
 * <p>
 * 该事务实现通常与 {@code SpringManagedTransactionFactory}
 * 或类似的事务工厂配合使用。
 * </p>
 *
 * @author aidan.liu
 * @since 2026/1/9
 */
public class ManagedTransaction implements Transaction {

  /**
   * 日志实例。
   */
  private static final Log log = LogFactory.getLog(ManagedTransaction.class);

  /**
   * 数据源。
   *
   * <p>
   * 当未直接传入 {@link Connection} 时，
   * 用于延迟获取连接。
   * </p>
   */
  private DataSource dataSource;

  /**
   * 期望设置的事务隔离级别。
   *
   * <p>
   * 在连接首次获取后设置，
   * 若为 {@code null} 则保持外部事务管理器的默认配置。
   * </p>
   */
  private TransactionIsolationLevel level;

  /**
   * 当前事务使用的 JDBC 连接。
   *
   * <p>
   * 可能由外部系统注入，
   * 也可能通过 {@link DataSource} 延迟获取。
   * </p>
   */
  private Connection connection;

  /**
   * 是否由 MyBatis 关闭连接。
   *
   * <p>
   * {@code true}：在 {@link #close()} 时关闭连接
   * {@code false}：连接的关闭由外部事务管理器负责
   * </p>
   */
  private final boolean closeConnection;

  /**
   * 使用已有 JDBC {@link Connection} 创建托管事务。
   *
   * <p>
   * 该构造方法通常用于外部事务管理器
   * 将连接直接注入 MyBatis 的场景。
   * </p>
   *
   * @param connection      已存在的 JDBC 连接
   * @param closeConnection 是否由 MyBatis 关闭连接
   */
  public ManagedTransaction(Connection connection, boolean closeConnection) {
    this.connection = connection;
    this.closeConnection = closeConnection;
  }

  /**
   * 使用数据源创建托管事务。
   *
   * <p>
   * 连接将在首次调用 {@link #getConnection()} 时获取。
   * </p>
   *
   * @param ds              数据源
   * @param level           期望的事务隔离级别
   * @param closeConnection 是否由 MyBatis 关闭连接
   */
  public ManagedTransaction(DataSource ds,
                            TransactionIsolationLevel level,
                            boolean closeConnection) {
    this.dataSource = ds;
    this.level = level;
    this.closeConnection = closeConnection;
  }

  /**
   * 获取当前事务关联的 JDBC {@link Connection}。
   *
   * <p>
   * 若连接尚未创建，则通过 {@link DataSource} 获取连接，
   * 并根据配置设置事务隔离级别。
   * </p>
   *
   * @return JDBC 连接
   * @throws SQLException 获取连接失败
   */
  @Override
  public Connection getConnection() throws SQLException {
    if (this.connection == null) {
      openConnection();
    }
    return this.connection;
  }

  /**
   * 提交事务。
   *
   * <p>
   * 在托管事务模式下，
   * 事务提交由外部事务管理器负责，
   * 因此该方法为<strong>空操作</strong>。
   * </p>
   */
  @Override
  public void commit() throws SQLException {
    // Does nothing
  }

  /**
   * 回滚事务。
   *
   * <p>
   * 与 {@link #commit()} 相同，
   * 回滚逻辑由外部事务管理器统一处理，
   * 因此该方法为<strong>空操作</strong>。
   * </p>
   */
  @Override
  public void rollback() throws SQLException {
    // Does nothing
  }

  /**
   * 关闭事务并释放资源。
   *
   * <p>
   * 是否真正关闭 {@link Connection}
   * 取决于 {@code closeConnection} 参数：
   * </p>
   * <ul>
   *   <li>{@code true}：由 MyBatis 关闭连接</li>
   *   <li>{@code false}：连接的生命周期由外部事务管理器控制</li>
   * </ul>
   *
   * @throws SQLException 关闭连接失败
   */
  @Override
  public void close() throws SQLException {
    if (this.closeConnection && this.connection != null) {
      if (log.isDebugEnabled()) {
        log.debug("Closing JDBC Connection [" + this.connection + "]");
      }
      this.connection.close();
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
   *   <li>设置事务隔离级别（如果指定）</li>
   * </ul>
   *
   * @throws SQLException 获取或初始化连接失败
   */
  protected void openConnection() throws SQLException {
    if (log.isDebugEnabled()) {
      log.debug("Opening JDBC Connection");
    }
    this.connection = this.dataSource.getConnection();
    if (this.level != null) {
      this.connection.setTransactionIsolation(this.level.getLevel());
    }
  }

  /**
   * 获取事务超时时间。
   *
   * <p>
   * 托管事务通常由外部系统控制超时，
   * 因此该方法默认返回 {@code null}。
   * </p>
   *
   * @return {@code null}
   */
  @Override
  public Integer getTimeout() throws SQLException {
    return null;
  }
}
