package org.my.test.transaction.managed;

import java.sql.Connection;
import java.util.Properties;
import javax.sql.DataSource;
import org.my.test.session.TransactionIsolationLevel;
import org.my.test.transaction.Transaction;
import org.my.test.transaction.TransactionFactory;

/**
 * 托管事务工厂，用于创建 {@link ManagedTransaction} 实例。
 *
 * <p>
 * {@code ManagedTransactionFactory} 适用于事务由外部系统
 * （如 Spring 事务管理器或 JTA 容器）统一管理的场景。
 * </p>
 *
 * <p>
 * 在该模式下，MyBatis 不负责事务的提交与回滚，
 * 仅负责获取和使用 {@link java.sql.Connection}。
 * </p>
 *
 * <p>
 * 是否由 MyBatis 关闭连接由 {@code closeConnection} 属性控制，
 * 该行为通常需要与外部事务管理器的连接管理策略保持一致。
 * </p>
 *
 * <p>
 * 典型配置方式（mybatis-config.xml）：
 * </p>
 *
 * <pre>
 * {@code
 * <transactionManager type="MANAGED">
 *   <property name="closeConnection" value="false"/>
 * </transactionManager>
 * }
 * </pre>
 *
 * @author aidan.liu
 * @since 2026/1/9
 */
public class ManagedTransactionFactory implements TransactionFactory {

  /**
   * 是否由 MyBatis 关闭 JDBC 连接。
   *
   * <p>
   * 默认值为 {@code true}，表示在事务结束时由 MyBatis 关闭连接。
   * </p>
   *
   * <p>
   * 在与 Spring 等框架集成时，
   * 通常需要将该值设置为 {@code false}，
   * 以避免 MyBatis 过早关闭由外部事务管理器维护的连接。
   * </p>
   */
  private boolean closeConnection = true;

  /**
   * 设置事务工厂的配置属性。
   *
   * <p>
   * 该方法在事务工厂初始化阶段调用，
   * 配置来源于 mybatis-config.xml 中的
   * {@code <transactionManager>} 节点。
   * </p>
   *
   * <p>
   * 当前支持的配置项：
   * </p>
   * <ul>
   *   <li>{@code closeConnection}：是否由 MyBatis 关闭连接</li>
   * </ul>
   *
   * @param props 事务工厂配置属性，可能为 {@code null}
   */
  @Override
  public void setProperties(Properties props) {
    if (props != null) {
      String closeConnectionProperty = props.getProperty("closeConnection");
      if (closeConnectionProperty != null) {
        closeConnection = Boolean.parseBoolean(closeConnectionProperty);
      }
    }
  }

  /**
   * 基于已有 JDBC {@link Connection} 创建托管事务。
   *
   * <p>
   * 该方法通常用于外部事务管理器
   * 将连接直接注入 MyBatis 的场景。
   * </p>
   *
   * <p>
   * {@code closeConnection} 参数将决定
   * MyBatis 是否在事务结束时关闭该连接。
   * </p>
   *
   * @param conn 已存在的 JDBC 连接
   * @return 托管事务实例
   */
  @Override
  public Transaction newTransaction(Connection conn) {
    return new ManagedTransaction(conn, closeConnection);
  }

  /**
   * 基于 {@link DataSource} 创建托管事务。
   *
   * <p>
   * 与 {@link JdbcTransactionFactory} 不同，
   * 该方法<strong>不会使用</strong> {@code autoCommit} 参数，
   * 因为 autoCommit 的控制权完全交由外部事务管理器。
   * </p>
   *
   * <p>
   * 事务隔离级别（如指定）会在连接获取后进行设置，
   * 但前提是外部事务管理器允许该操作。
   * </p>
   *
   * @param ds 数据源
   * @param level 事务隔离级别，可能为 {@code null}
   * @param autoCommit 是否启用 autoCommit（在托管事务中被忽略）
   * @return 托管事务实例
   */
  @Override
  public Transaction newTransaction(
      DataSource ds,
      TransactionIsolationLevel level,
      boolean autoCommit) {

    return new ManagedTransaction(ds, level, closeConnection);
  }
}

