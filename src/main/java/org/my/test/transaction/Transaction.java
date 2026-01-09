package org.my.test.transaction;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * 事务接口，定义 MyBatis 对事务行为的最小抽象。
 *
 * <p>
 * 该接口用于屏蔽底层事务实现的差异（JDBC、Spring 托管事务、JTA 等），
 * 为执行器（Executor）提供统一的事务访问与控制能力。
 * </p>
 *
 * <p>
 * {@code Transaction} 的核心职责包括：
 * </p>
 * <ul>
 *   <li>提供当前事务关联的 {@link Connection}</li>
 *   <li>控制事务的提交与回滚</li>
 *   <li>管理连接的关闭与资源释放</li>
 *   <li>提供事务级别的可选超时信息</li>
 * </ul>
 *
 * <p>
 * 该接口本身<strong>不定义事务边界</strong>，
 * 事务的开启与结束时机由上层组件（如 {@code SqlSession}、{@code Executor}）
 * 负责协调。
 * </p>
 *
 * <p>
 * 不同实现对以下行为可能有不同策略：
 * </p>
 * <ul>
 *   <li>{@link #commit()} / {@link #rollback()} 是否实际调用 JDBC API</li>
 *   <li>{@link #close()} 是否真正关闭 {@link Connection}</li>
 *   <li>{@link #getTimeout()} 是否返回有效的超时值</li>
 * </ul>
 *
 * @author aidan.liu
 * @since 2026/1/9
 */
public interface Transaction {

  /**
   * 获取当前事务关联的 JDBC {@link Connection}。
   *
   * <p>
   * 该方法可能返回：
   * </p>
   * <ul>
   *   <li>已存在的连接（如外部事务传入）</li>
   *   <li>通过 {@link javax.sql.DataSource} 延迟创建的新连接</li>
   * </ul>
   *
   * <p>
   * 调用该方法通常意味着事务即将开始执行 SQL。
   * </p>
   *
   * @return 当前事务使用的 JDBC 连接
   * @throws SQLException 获取连接失败
   */
  Connection getConnection() throws SQLException;

  /**
   * 提交当前事务。
   *
   * <p>
   * 实现类应根据自身的事务管理策略决定是否执行实际提交操作：
   * </p>
   * <ul>
   *   <li>JDBC 事务：调用 {@link Connection#commit()}</li>
   *   <li>Spring 托管事务：可能为空操作</li>
   *   <li>JTA 事务：由容器负责提交</li>
   * </ul>
   *
   * @throws SQLException 提交失败
   */
  void commit() throws SQLException;

  /**
   * 回滚当前事务。
   *
   * <p>
   * 与 {@link #commit()} 类似，
   * 实现类可能根据事务类型选择性地执行回滚逻辑。
   * </p>
   *
   * @throws SQLException 回滚失败
   */
  void rollback() throws SQLException;

  /**
   * 关闭事务并释放相关资源。
   *
   * <p>
   * 该方法用于结束事务生命周期，
   * 具体行为由实现类决定：
   * </p>
   * <ul>
   *   <li>直接关闭 JDBC {@link Connection}</li>
   *   <li>将连接归还给连接池</li>
   *   <li>在托管事务环境下可能为空操作</li>
   * </ul>
   *
   * <p>
   * 该方法通常由 {@code Executor} 或 {@code SqlSession} 在 finally 块中调用。
   * </p>
   *
   * @throws SQLException 关闭失败
   */
  void close() throws SQLException;

  /**
   * 获取事务超时时间（秒）。
   *
   * <p>
   * 该方法为可选能力，
   * 并非所有事务实现都支持超时控制。
   * </p>
   *
   * <p>
   * 返回值语义：
   * </p>
   * <ul>
   *   <li>{@code null}：未配置或不支持事务超时</li>
   *   <li>{@code > 0}：事务剩余可用时间（秒）</li>
   * </ul>
   *
   * <p>
   * 上层组件（如 {@code StatementHandler}）
   * 可使用该值设置 {@code Statement#setQueryTimeout(int)}。
   * </p>
   *
   * @return 事务超时时间（秒），或 {@code null}
   * @throws SQLException 获取超时信息失败
   */
  Integer getTimeout() throws SQLException;
}

