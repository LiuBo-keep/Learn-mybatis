package org.my.test.datasource.pooled;

import java.io.PrintWriter;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;
import javax.sql.DataSource;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.my.test.datasource.unpooled.UnpooledDataSource;

/**
 * 连接池数据源实现类
 * 实现DataSource接口，提供数据库连接池功能，用于管理和复用数据库连接
 * 通过连接池机制提高数据库连接的使用效率，减少频繁创建和销毁连接的开销
 *
 * @author aidan.liu
 * @version 1.0
 * @since 2026/1/9 14:15
 */
@Slf4j
@Data
public class PooledDataSource implements DataSource {

  private final PoolState state = new PoolState(this);

  private final UnpooledDataSource dataSource;


  /**
   * 连接池最大活跃连接数，默认值为10
   */
  protected int poolMaximumActiveConnections = 10;

  /**
   * 连接池最大空闲连接数，默认值为5
   */
  protected int poolMaximumIdleConnections = 5;

  /**
   * 连接池最大检出时间（毫秒），超过此时间未归还连接将被强制回收，默认值为20000毫秒
   */
  protected int poolMaximumCheckoutTime = 20000;

  /**
   * 连接池等待时间（毫秒），当没有可用连接时等待的时间，默认值为20000毫秒
   */
  protected int poolTimeToWait = 20000;

  /**
   * 连接池本地坏连接容忍次数，允许连续获取坏连接的最大次数，默认值为3
   */
  protected int poolMaximumLocalBadConnectionTolerance = 3;

  /**
   * 连接池心跳检测查询语句，默认值为"NO PING QUERY SET"
   */
  protected String poolPingQuery = "NO PING QUERY SET";

  /**
   * 连接池心跳检测是否启用
   */
  protected boolean poolPingEnabled;

  /**
   * 连接池心跳检测连接未使用时间阈值（秒）
   */
  protected int poolPingConnectionsNotUsedFor;

  /**
   * 预期的连接类型编码，用于验证连接类型的匹配性
   */
  private int expectedConnectionTypeCode;


  private final Lock lock = new ReentrantLock();
  private final Condition condition = lock.newCondition();

  public PooledDataSource() {
    dataSource = new UnpooledDataSource();
  }

  public PooledDataSource(UnpooledDataSource dataSource) {
    this.dataSource = dataSource;
  }

  public PooledDataSource(String driver, String url, String username, String password) {
    dataSource = new UnpooledDataSource(driver, url, username, password);
    expectedConnectionTypeCode = assembleConnectionTypeCode(dataSource.getUrl(), dataSource.getUsername(),
        dataSource.getPassword());
  }

  public PooledDataSource(String driver, String url, Properties driverProperties) {
    dataSource = new UnpooledDataSource(driver, url, driverProperties);
    expectedConnectionTypeCode = assembleConnectionTypeCode(dataSource.getUrl(), dataSource.getUsername(),
        dataSource.getPassword());
  }

  public PooledDataSource(ClassLoader driverClassLoader, String driver, String url, String username, String password) {
    dataSource = new UnpooledDataSource(driverClassLoader, driver, url, username, password);
    expectedConnectionTypeCode = assembleConnectionTypeCode(dataSource.getUrl(), dataSource.getUsername(),
        dataSource.getPassword());
  }

  public PooledDataSource(ClassLoader driverClassLoader, String driver, String url, Properties driverProperties) {
    dataSource = new UnpooledDataSource(driverClassLoader, driver, url, driverProperties);
    expectedConnectionTypeCode = assembleConnectionTypeCode(dataSource.getUrl(), dataSource.getUsername(),
        dataSource.getPassword());
  }

  @Override
  public Connection getConnection() throws SQLException {
    return popConnection(dataSource.getUsername(), dataSource.getPassword()).getProxyConnection();
  }

  @Override
  public Connection getConnection(String username, String password) throws SQLException {
    return popConnection(username, password).getProxyConnection();
  }

  @Override
  public PrintWriter getLogWriter() throws SQLException {
    return DriverManager.getLogWriter();
  }

  @Override
  public void setLogWriter(PrintWriter out) throws SQLException {
    DriverManager.setLogWriter(out);
  }

  @Override
  public void setLoginTimeout(int seconds) throws SQLException {
    DriverManager.setLoginTimeout(seconds);
  }

  @Override
  public int getLoginTimeout() throws SQLException {
    return DriverManager.getLoginTimeout();
  }

  public void setDriver(String driver) {
    dataSource.setDriver(driver);
    forceCloseAll();
  }

  public String getDriver() {
    return dataSource.getDriver();
  }

  public void setUrl(String url) {
    dataSource.setUrl(url);
    forceCloseAll();
  }

  public String getUrl() {
    return dataSource.getUrl();
  }

  public void setUsername(String username) {
    dataSource.setUsername(username);
    forceCloseAll();
  }

  public String getUsername() {
    return dataSource.getUsername();
  }

  public void setPassword(String password) {
    dataSource.setPassword(password);
    forceCloseAll();
  }

  public String getPassword() {
    return dataSource.getPassword();
  }

  public void setDefaultAutoCommit(boolean defaultAutoCommit) {
    dataSource.setAutoCommit(defaultAutoCommit);
    forceCloseAll();
  }

  public void setDefaultTransactionIsolationLevel(Integer defaultTransactionIsolationLevel) {
    dataSource.setDefaultTransactionIsolationLevel(defaultTransactionIsolationLevel);
    forceCloseAll();
  }

  public void setDriverProperties(Properties driverProps) {
    dataSource.setDriverProperties(driverProps);
    forceCloseAll();
  }

  public void setDefaultNetworkTimeout(Integer milliseconds) {
    dataSource.setDefaultNetworkTimeout(milliseconds);
    forceCloseAll();
  }

  public void setPoolMaximumActiveConnections(int poolMaximumActiveConnections) {
    this.poolMaximumActiveConnections = poolMaximumActiveConnections;
    forceCloseAll();
  }

  public void setPoolMaximumIdleConnections(int poolMaximumIdleConnections) {
    this.poolMaximumIdleConnections = poolMaximumIdleConnections;
    forceCloseAll();
  }

  public void setPoolMaximumCheckoutTime(int poolMaximumCheckoutTime) {
    this.poolMaximumCheckoutTime = poolMaximumCheckoutTime;
    forceCloseAll();
  }

  public void setPoolTimeToWait(int poolTimeToWait) {
    this.poolTimeToWait = poolTimeToWait;
    forceCloseAll();
  }

  public void setPoolPingQuery(String poolPingQuery) {
    this.poolPingQuery = poolPingQuery;
    forceCloseAll();
  }

  public void setPoolPingEnabled(boolean poolPingEnabled) {
    this.poolPingEnabled = poolPingEnabled;
    forceCloseAll();
  }

  public void setPoolPingConnectionsNotUsedFor(int milliseconds) {
    this.poolPingConnectionsNotUsedFor = milliseconds;
    forceCloseAll();
  }

  public boolean isAutoCommit() {
    return dataSource.isAutoCommit();
  }

  public Integer getDefaultTransactionIsolationLevel() {
    return dataSource.getDefaultTransactionIsolationLevel();
  }

  public Properties getDriverProperties() {
    return dataSource.getDriverProperties();
  }

  public Integer getDefaultNetworkTimeout() {
    return dataSource.getDefaultNetworkTimeout();
  }

  /**
   * 强制关闭所有连接，包括活跃连接和空闲连接
   * <p>
   * 该方法用于彻底清理连接池中的所有连接，在数据源配置发生变化或应用关闭时调用
   * 操作流程：
   * 1. 更新预期连接类型编码以匹配新的配置
   * 2. 遍历并关闭所有活跃连接
   * 3. 遍历并关闭所有空闲连接
   * 4. 记录操作日志
   * <p>
   * 该方法是线程安全的，使用锁保证操作的原子性
   */
  public void forceCloseAll() {
    lock.lock();
    try {
      // 重新组装连接类型编码，以适应可能变更的数据源配置
      expectedConnectionTypeCode = assembleConnectionTypeCode(dataSource.getUrl(), dataSource.getUsername(),
          dataSource.getPassword());
      // 遍历并关闭所有活跃连接
      for (int i = state.activeConnections.size(); i > 0; i--) {
        try {
          // 从活跃连接列表中移除连接
          PooledConnection conn = state.activeConnections.remove(i - 1);
          // 标记连接为无效状态
          conn.invalidate();

          // 获取真实的数据库连接
          Connection realConn = conn.getRealConnection();
          // 如果连接未处于自动提交模式，先回滚未提交的事务
          if (!realConn.getAutoCommit()) {
            realConn.rollback();
          }
          // 关闭真实的数据库连接
          realConn.close();
        } catch (Exception e) {
          // ignore
        }
      }
      // 遍历并关闭所有空闲连接
      for (int i = state.idleConnections.size(); i > 0; i--) {
        try {
          // 从空闲连接列表中移除连接
          PooledConnection conn = state.idleConnections.remove(i - 1);
          // 标记连接为无效状态
          conn.invalidate();

          // 获取真实的数据库连接
          Connection realConn = conn.getRealConnection();
          // 如果连接未处于自动提交模式，先回滚未提交的事务
          if (!realConn.getAutoCommit()) {
            realConn.rollback();
          }
          // 关闭真实的数据库连接
          realConn.close();
        } catch (Exception e) {
          // ignore
        }
      }
    } finally {
      lock.unlock();
    }
    // 记录调试日志，说明所有连接已被强制关闭
    if (log.isDebugEnabled()) {
      log.debug("PooledDataSource forcefully closed/removed all connections.");
    }
  }

  /**
   * 将连接返回到连接池中
   * <p>
   * 该方法处理应用程序归还连接到连接池的逻辑，是连接池实现的关键部分。
   * 根据连接的有效性、空闲连接池容量和连接类型匹配情况，决定将连接放回空闲池或直接关闭。
   * <p>
   * 处理流程：
   * 1. 从活跃连接列表中移除连接
   * 2. 验证连接是否有效
   * 3. 根据空闲池容量和连接类型决定处理方式
   * 4. 对于无效连接进行计数统计
   * <p>
   * 该方法是线程安全的，使用锁保证操作的原子性
   *
   * @param conn 要返回到连接池的PooledConnection对象，不能为null
   * @throws SQLException 当数据库操作出现异常时抛出
   */
  protected void pushConnection(PooledConnection conn) throws SQLException {

    // 获取锁以保证线程安全地操作连接池状态
    lock.lock();
    try {
      // 从活跃连接列表中移除该连接，因为它正在被归还
      state.activeConnections.remove(conn);

      // 检查连接是否仍然有效
      if (conn.isValid()) {
        // 检查是否可以将连接放回空闲池（容量未满且连接类型匹配）
        if (state.idleConnections.size() < poolMaximumIdleConnections
            && conn.getConnectionTypeCode() == expectedConnectionTypeCode) {
          // 累加连接的检出时间到统计信息中
          state.accumulatedCheckoutTime += conn.getCheckoutTime();

          // 如果连接未处于自动提交模式，回滚可能存在的未提交事务
          // 这是为了避免事务状态影响后续使用该连接的操作
          if (!conn.getRealConnection().getAutoCommit()) {
            conn.getRealConnection().rollback();
          }

          // 创建新的连接包装对象，避免连接对象被重复使用
          PooledConnection newConn = new PooledConnection(conn.getRealConnection(), this);

          // 将新创建的连接添加到空闲连接列表中
          state.idleConnections.add(newConn);

          // 复制原始连接的时间戳信息，保持连接生命周期数据的一致性
          newConn.setCreatedTimestamp(conn.getCreatedTimestamp());
          newConn.setLastUsedTimestamp(conn.getLastUsedTimestamp());

          // 使原连接失效，防止被意外使用
          conn.invalidate();

          // 记录调试日志，显示连接已成功返回到池中
          if (log.isDebugEnabled()) {
            log.debug("Returned connection " + newConn.getRealHashCode() + " to pool.");
          }

          // 发送信号通知等待的线程（如果有线程在等待可用连接）
          condition.signal();
        } else {
          // 空闲池已满或连接类型不匹配，直接关闭连接

          // 累加连接的检出时间到统计信息中
          state.accumulatedCheckoutTime += conn.getCheckoutTime();

          // 回滚可能存在的未提交事务
          if (!conn.getRealConnection().getAutoCommit()) {
            conn.getRealConnection().rollback();
          }

          // 直接关闭物理连接，释放数据库资源
          conn.getRealConnection().close();

          // 记录调试日志，显示连接已被关闭
          if (log.isDebugEnabled()) {
            log.debug("Closed connection " + conn.getRealHashCode() + ".");
          }

          // 使连接失效
          conn.invalidate();
        }
      } else {
        // 连接无效，记录到坏连接计数器中用于统计
        if (log.isDebugEnabled()) {
          log.debug("A bad connection (" + conn.getRealHashCode()
              + ") attempted to return to the pool, discarding connection.");
        }
        // 增加坏连接计数，用于监控连接池健康状况
        state.badConnectionCount++;
      }
    } finally {
      // 释放锁，确保锁的正确释放
      lock.unlock();
    }
  }


  /**
   * 从连接池中获取一个可用的数据库连接
   * <p>
   * 该方法按照以下优先级获取连接：
   * 1. 优先从空闲连接列表中获取可用连接
   * 2. 如果没有空闲连接且活跃连接数未达到最大限制，则创建新连接
   * 3. 如果活跃连接数已达到最大限制，则尝试回收超时的连接
   * 4. 如果没有超时连接，则等待指定时间后重试
   * <p>
   * 同时处理连接的有效性验证、事务回滚、连接类型匹配等逻辑
   *
   * @param username 数据库用户名，用于连接类型验证
   * @param password 数据库密码，用于连接类型验证
   * @return 从连接池中获取的PooledConnection对象，已验证有效且被标记为活跃状态
   * @throws SQLException 当无法获取有效连接或达到坏连接容忍上限时抛出异常
   */
  private PooledConnection popConnection(String username, String password) throws SQLException {
    // 标记是否已经计算过等待次数，用于避免重复统计
    boolean countedWait = false;
    // 要返回的连接对象，初始为null，通过循环不断尝试获取
    PooledConnection conn = null;
    // 记录开始时间，用于统计获取连接的总耗时
    long t = System.currentTimeMillis();
    // 本地坏连接计数器，用于限制连续获取坏连接的次数
    int localBadConnectionCount = 0;

    // 循环直到成功获取一个有效连接或达到错误条件
    while (conn == null) {
      lock.lock();
      try {
        // 情况1：检查是否有空闲连接可用
        if (!state.idleConnections.isEmpty()) {
          // 从空闲连接列表中取出第一个连接
          conn = state.idleConnections.remove(0);
          if (log.isDebugEnabled()) {
            log.debug("从连接池检出连接 " + conn.getRealHashCode());
          }
        }
        // 情况2：如果没有空闲连接但活跃连接数未达到最大限制
        else if (state.activeConnections.size() < poolMaximumActiveConnections) {
          // 通过底层数据源创建新的物理连接并包装为池化连接
          conn = new PooledConnection(dataSource.getConnection(), this);
          if (log.isDebugEnabled()) {
            log.debug("创建连接 " + conn.getRealHashCode());
          }
        }
        // 情况3：没有空闲连接且活跃连接数已达到最大限制
        else {
          // 获取最老的活跃连接（按检出时间排序）
          PooledConnection oldestActiveConnection = state.activeConnections.get(0);
          // 计算该连接的检出时间
          long longestCheckoutTime = oldestActiveConnection.getCheckoutTime();
          // 检查该连接是否超时（检出时间超过最大允许时间）
          if (longestCheckoutTime > poolMaximumCheckoutTime) {
            // 统计超时连接回收次数和时间
            state.claimedOverdueConnectionCount++;
            state.accumulatedCheckoutTimeOfOverdueConnections += longestCheckoutTime;
            state.accumulatedCheckoutTime += longestCheckoutTime;

            // 从活跃连接列表中移除超时连接
            state.activeConnections.remove(oldestActiveConnection);
            // 如果超时连接有未提交的事务，尝试回滚
            if (!oldestActiveConnection.getRealConnection().getAutoCommit()) {
              try {
                oldestActiveConnection.getRealConnection().rollback();
              } catch (SQLException e) {
                /*
                 * 仅记录调试日志，继续执行后续操作
                 * 将坏连接包装为新连接，避免中断当前线程
                 * 给当前线程机会参与下一次获取有效连接的竞争
                 * 在循环结束时，坏连接conn将被设置为null
                 */
                log.debug("坏连接。无法回滚");
              }
            }
            // 使用超时连接的物理连接创建新的池化连接
            conn = new PooledConnection(oldestActiveConnection.getRealConnection(), this);
            // 复制创建时间和最后使用时间
            conn.setCreatedTimestamp(oldestActiveConnection.getCreatedTimestamp());
            conn.setLastUsedTimestamp(oldestActiveConnection.getLastUsedTimestamp());
            // 使原连接失效
            oldestActiveConnection.invalidate();
            if (log.isDebugEnabled()) {
              log.debug("回收超时连接 " + conn.getRealHashCode());
            }
          } else {
            // 情况4：没有超时连接，需要等待
            try {
              // 只在第一次等待时增加等待计数
              if (!countedWait) {
                state.hadToWaitCount++;
                countedWait = true;
              }
              if (log.isDebugEnabled()) {
                log.debug("等待最多 " + poolTimeToWait + " 毫秒以获取连接");
              }
              // 记录等待开始时间
              long wt = System.currentTimeMillis();
              // 等待指定时间，如果超时则继续下一次循环
              if (!condition.await(poolTimeToWait, TimeUnit.MILLISECONDS)) {
                log.debug("等待失败...");
              }
              // 统计总等待时间
              state.accumulatedWaitTime += System.currentTimeMillis() - wt;
            } catch (InterruptedException e) {
              // 设置中断标志并跳出循环
              Thread.currentThread().interrupt();
              break;
            }
          }
        }
        // 如果成功获取了连接对象
        if (conn != null) {
          // 验证连接是否有效（包括ping检查）
          if (conn.isValid()) {
            // 如果连接未自动提交，先回滚未提交的事务
            if (!conn.getRealConnection().getAutoCommit()) {
              conn.getRealConnection().rollback();
            }
            // 设置连接类型编码用于后续验证
            conn.setConnectionTypeCode(assembleConnectionTypeCode(dataSource.getUrl(), username, password));
            // 设置检出时间戳和最后使用时间戳
            conn.setCheckoutTimestamp(System.currentTimeMillis());
            conn.setLastUsedTimestamp(System.currentTimeMillis());
            // 将连接添加到活跃连接列表
            state.activeConnections.add(conn);
            // 增加请求计数和累计请求时间
            state.requestCount++;
            state.accumulatedRequestTime += System.currentTimeMillis() - t;
          } else {
            // 连接无效时的处理
            if (log.isDebugEnabled()) {
              log.debug("从连接池返回了一个坏连接 (" + conn.getRealHashCode() + ")，正在获取另一个连接");
            }
            // 增加全局坏连接计数
            state.badConnectionCount++;
            // 增加本地坏连接计数
            localBadConnectionCount++;
            // 将连接置空，继续循环获取
            conn = null;
            // 如果连续获取坏连接次数超过容忍上限，抛出异常
            if (localBadConnectionCount > poolMaximumIdleConnections + poolMaximumLocalBadConnectionTolerance) {
              if (log.isDebugEnabled()) {
                log.debug("PooledDataSource: 无法获取到有效的数据库连接");
              }
              throw new SQLException("PooledDataSource: 无法获取到有效的数据库连接");
            }
          }
        }
      } finally {
        // 释放锁，保证锁的正确释放
        lock.unlock();
      }

    }

    // 如果循环结束仍未获取到连接，抛出异常
    if (conn == null) {
      if (log.isDebugEnabled()) {
        log.debug("PooledDataSource: 未知严重错误条件。连接池返回了空连接");
      }
      throw new SQLException("PooledDataSource: 未知严重错误条件。连接池返回了空连接");
    }

    // 返回获取到的有效连接
    return conn;
  }

  /**
   * 检查连接池中的连接是否有效
   * <p>
   * 该方法用于验证连接池中的连接是否仍然有效，主要通过两步验证：
   * 1. 首先检查连接是否已关闭
   * 2. 如果连接未关闭且启用了ping功能，则执行ping查询来进一步验证连接的有效性
   * <p>
   * 该方法支持心跳检测功能，对于长时间未使用的连接会执行ping查询验证
   *
   * @param conn 要检查的连接池连接对象，包含真实连接和连接元数据
   * @return 如果连接有效返回true，否则返回false
   */
  protected boolean pingConnection(PooledConnection conn) {
    // 初始化结果变量
    boolean result;

    // 首先检查连接是否已关闭
    try {
      result = !conn.getRealConnection().isClosed();
    } catch (SQLException e) {
      // 如果检查过程中发生异常，记录调试日志并标记连接为无效
      if (log.isDebugEnabled()) {
        log.debug("Connection " + conn.getRealHashCode() + " is BAD: " + e.getMessage());
      }
      result = false;
    }

    // 如果连接当前有效且启用了ping功能，并且连接空闲时间超过指定阈值，则执行ping查询验证
    // 这里的判断条件包括：
    // - result: 连接当前被认为是有效的
    // - poolPingEnabled: ping功能已启用
    // - poolPingConnectionsNotUsedFor >= 0: ping时间阈值大于等于0
    // - conn.getTimeElapsedSinceLastUse() > poolPingConnectionsNotUsedFor: 连接空闲时间超过阈值
    if (result && poolPingEnabled && poolPingConnectionsNotUsedFor >= 0
        && conn.getTimeElapsedSinceLastUse() > poolPingConnectionsNotUsedFor) {
      try {
        // 记录调试日志，表示正在进行连接测试
        if (log.isDebugEnabled()) {
          log.debug("Testing connection " + conn.getRealHashCode() + " ...");
        }
        // 获取真实连接对象
        Connection realConn = conn.getRealConnection();
        // 创建并执行ping查询，验证连接是否仍然可用
        try (Statement statement = realConn.createStatement()) {
          statement.executeQuery(poolPingQuery).close();
        }
        // 如果连接不是自动提交模式，回滚ping查询可能产生的事务
        if (!realConn.getAutoCommit()) {
          realConn.rollback();
        }
        // 连接验证成功，记录调试日志
        if (log.isDebugEnabled()) {
          log.debug("Connection " + conn.getRealHashCode() + " is GOOD!");
        }
      } catch (Exception e) {
        // ping查询失败，记录警告日志
        log.warn("Execution of ping query '" + poolPingQuery + "' failed: " + e.getMessage());
        // 尝试关闭连接以释放资源
        try {
          conn.getRealConnection().close();
        } catch (Exception e2) {
          // 忽略关闭连接时的异常
        }
        // 标记连接为无效
        result = false;
        // 记录调试日志，说明连接无效
        if (log.isDebugEnabled()) {
          log.debug("Connection " + conn.getRealHashCode() + " is BAD: " + e.getMessage());
        }
      }
    }
    // 返回最终的连接有效性判断结果
    return result;
  }


  public static Connection unwrapConnection(Connection conn) {
    if (Proxy.isProxyClass(conn.getClass())) {
      InvocationHandler handler = Proxy.getInvocationHandler(conn);
      if (handler instanceof PooledConnection) {
        return ((PooledConnection) handler).getRealConnection();
      }
    }
    return conn;
  }

  @Override
  protected void finalize() throws Throwable {
    forceCloseAll();
    super.finalize();
  }

  private int assembleConnectionTypeCode(String url, String username, String password) {
    return ("" + url + username + password).hashCode();
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new SQLException(getClass().getName() + " is not a wrapper.");
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return false;
  }
}
