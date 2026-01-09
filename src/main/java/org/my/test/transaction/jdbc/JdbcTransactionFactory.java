package org.my.test.transaction.jdbc;

import java.sql.Connection;
import java.util.Properties;
import javax.sql.DataSource;
import org.my.test.session.TransactionIsolationLevel;
import org.my.test.transaction.Transaction;
import org.my.test.transaction.TransactionFactory;

/**
 * JDBC 事务工厂实现。
 *
 * <p>
 * 该类用于创建 {@link JdbcTransaction} 实例，是 MyBatis 默认的事务工厂实现之一。
 * 通过该工厂，可以基于已有的 {@link Connection} 或 {@link DataSource} 创建 JDBC 事务。
 * </p>
 *
 * <p>
 * 支持通过属性 {@code skipSetAutoCommitOnClose} 控制在事务关闭时是否跳过
 * {@link Connection#setAutoCommit(boolean)} 的重置操作。
 * 该参数在与连接池（如 HikariCP、Druid）协作时尤为重要，
 * 可避免因重复设置 autoCommit 而带来的性能损耗或异常行为。
 * </p>
 *
 * <p>
 * 典型配置方式（mybatis-config.xml）：
 * </p>
 *
 * <pre>
 * {@code
 * <transactionManager type="JDBC">
 *   <property name="skipSetAutoCommitOnClose" value="true"/>
 * </transactionManager>
 * }
 * </pre>
 *
 * @author aidan.liu
 * @since 2026/1/9
 */
public class JdbcTransactionFactory implements TransactionFactory {

  /**
   * 是否在事务关闭时跳过 autoCommit 重置。
   *
   * <p>
   * 默认值为 {@code false}，即在事务关闭时会尝试将
   * {@link Connection#setAutoCommit(boolean)} 恢复为 {@code true}。
   * </p>
   *
   * <p>
   * 当使用连接池并由连接池统一管理 autoCommit 状态时，
   * 可以将该值设置为 {@code true}，以避免不必要的 JDBC 调用。
   * </p>
   */
  private boolean skipSetAutoCommitOnClose;

  /**
   * 设置事务工厂的配置属性。
   *
   * <p>
   * 该方法由 MyBatis 在初始化事务管理器时调用，
   * 属性来源于 mybatis-config.xml 中 {@code <transactionManager>} 节点。
   * </p>
   *
   * <p>
   * 当前支持的配置项：
   * </p>
   * <ul>
   *   <li>{@code skipSetAutoCommitOnClose}：是否在关闭事务时跳过 autoCommit 重置</li>
   * </ul>
   *
   * @param props 事务工厂的配置属性，可能为 {@code null}
   */
  @Override
  public void setProperties(Properties props) {
    if (props == null) {
      return;
    }
    String value = props.getProperty("skipSetAutoCommitOnClose");
    if (value != null) {
      skipSetAutoCommitOnClose = Boolean.parseBoolean(value);
    }
  }

  /**
   * 基于已有的 JDBC {@link Connection} 创建事务。
   *
   * <p>
   * 该方法通常用于外部事务管理场景，
   * 如 Spring 管理的事务或用户自行获取并管理 Connection 的情况。
   * </p>
   *
   * <p>
   * 在此模式下，事务的生命周期由外部控制，
   * MyBatis 仅对事务进行简单封装，不会更改 autoCommit 行为。
   * </p>
   *
   * @param conn 已存在的 JDBC 连接
   * @return 基于该连接的 {@link Transaction} 实例
   */
  @Override
  public Transaction newTransaction(Connection conn) {
    return new JdbcTransaction(conn);
  }

  /**
   * 基于 {@link DataSource} 创建新的 JDBC 事务。
   *
   * <p>
   * 该方法是 MyBatis 内部最常用的事务创建方式，
   * 会从数据源中获取连接，并根据参数设置事务隔离级别与 autoCommit 状态。
   * </p>
   *
   * <p>
   * {@code skipSetAutoCommitOnClose} 参数会传递给 {@link JdbcTransaction}，
   * 用于控制在事务关闭时是否跳过 autoCommit 重置操作。
   * </p>
   *
   * @param ds         数据源
   * @param level      事务隔离级别，可能为 {@code null}
   * @param autoCommit 是否启用 autoCommit
   * @return 新创建的 {@link Transaction} 实例
   */
  @Override
  public Transaction newTransaction(
      DataSource ds,
      TransactionIsolationLevel level,
      boolean autoCommit) {

    return new JdbcTransaction(ds, level, autoCommit, skipSetAutoCommitOnClose);
  }
}

