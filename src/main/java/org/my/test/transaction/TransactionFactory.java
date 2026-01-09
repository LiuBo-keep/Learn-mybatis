package org.my.test.transaction;

import java.sql.Connection;
import java.util.Properties;
import javax.sql.DataSource;
import org.my.test.session.TransactionIsolationLevel;

/**
 * 事务工厂接口，用于创建 {@link Transaction} 实例。
 *
 * <p>
 * {@code TransactionFactory} 是 MyBatis 事务体系中的创建入口，
 * 负责根据运行环境和配置方式生成合适的 {@link Transaction} 实现。
 * </p>
 *
 * <p>
 * 通过该接口，MyBatis 可以在不感知具体事务实现细节的情况下，
 * 支持多种事务模型（JDBC、本地事务、Spring 托管事务、JTA 等）。
 * </p>
 *
 * <p>
 * 事务工厂通常由 {@code Environment} 持有，
 * 并在 {@code SqlSession} 创建过程中被调用。
 * </p>
 *
 * @author aidan.liu
 * @since 2026/1/9
 */
public interface TransactionFactory {

  /**
   * 设置事务工厂的配置属性。
   *
   * <p>
   * 该方法在事务工厂实例化后、首次创建事务前调用，
   * 配置来源于 mybatis-config.xml 中的 {@code <transactionManager>} 节点。
   * </p>
   *
   * <p>
   * 该方法被定义为 default 且默认实现为空操作（NOP），
   * 是为了允许具体实现类按需解析配置，而不强制所有实现支持配置。
   * </p>
   *
   * <p>
   * 常见用途包括：
   * </p>
   * <ul>
   *   <li>解析是否跳过 autoCommit 重置</li>
   *   <li>配置连接获取或关闭行为</li>
   * </ul>
   *
   * @param props 事务工厂配置属性，可能为 {@code null}
   */
  default void setProperties(Properties props) {
    // NOP
  }

  /**
   * 基于已有 JDBC {@link Connection} 创建事务。
   *
   * <p>
   * 该方法主要用于外部事务管理场景，
   * 如 Spring 管理的事务或用户自行获取并管理连接的情况。
   * </p>
   *
   * <p>
   * 在此模式下，连接的生命周期和事务边界通常由外部系统控制，
   * {@link Transaction} 实现可能不会真正关闭连接。
   * </p>
   *
   * @param conn 已存在的 JDBC 连接
   * @return 基于该连接的 {@link Transaction} 实例
   */
  Transaction newTransaction(Connection conn);

  /**
   * 基于 {@link DataSource} 创建新的事务。
   *
   * <p>
   * 该方法是 MyBatis 内部最常用的事务创建方式，
   * 事务工厂负责：
   * </p>
   * <ul>
   *   <li>从数据源获取连接</li>
   *   <li>设置事务隔离级别</li>
   *   <li>配置 autoCommit 行为</li>
   * </ul>
   *
   * <p>
   * 事务的实际生命周期管理由返回的 {@link Transaction} 实现负责。
   * </p>
   *
   * @param dataSource 数据源
   * @param level      事务隔离级别，可能为 {@code null}
   * @param autoCommit 是否启用 autoCommit
   * @return 新创建的 {@link Transaction} 实例
   */
  Transaction newTransaction(
      DataSource dataSource,
      TransactionIsolationLevel level,
      boolean autoCommit);
}

