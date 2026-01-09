package org.my.test.datasource.pooled;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 连接池状态管理类
 * <p>
 * 该类负责维护和管理数据库连接池的运行时状态，包括活跃连接和空闲连接列表，
 * 以及各种性能指标统计数据。通过ReentrantLock保证线程安全访问共享状态数据。
 * <p>
 * 注意：该锁不能完全保证一致性，因为字段值可能在PooledDataSource中被修改
 * 在从PooledDataSource#getPoolState()返回实例之后。可能的修复方案是创建并返回快照。
 * <p>
 * 主要功能：
 * - 维护活跃连接和空闲连接列表
 * - 统计连接池性能指标
 * - 提供线程安全的状态访问方法
 *
 * @author aidan.liu
 * @version 1.0
 * @since 2026/1/9 14:11
 */
public class PoolState {
  // 线程安全锁，用于保护状态数据的并发访问
  private final ReentrantLock lock = new ReentrantLock();
  // 关联的连接池数据源对象
  protected PooledDataSource dataSource;

  // 空闲连接列表，存储当前未被使用的连接
  protected final List<PooledConnection> idleConnections = new ArrayList<>();
  // 活跃连接列表，存储当前正在被使用的连接
  protected final List<PooledConnection> activeConnections = new ArrayList<>();
  // 请求总数，记录连接池被请求的次数
  protected long requestCount;
  // 累计请求时间，记录所有请求的总耗时
  protected long accumulatedRequestTime;
  // 累计检出时间，记录连接被检出使用的总时间
  protected long accumulatedCheckoutTime;
  // 已声明的超时连接数量，记录因超时被强制回收的连接数
  protected long claimedOverdueConnectionCount;
  // 超时连接的累计检出时间，记录超时连接的总检出时间
  protected long accumulatedCheckoutTimeOfOverdueConnections;
  // 累计等待时间，记录连接池等待分配连接的总时间
  protected long accumulatedWaitTime;
  // 需要等待的次数，记录连接池因无可用连接而等待的次数
  protected long hadToWaitCount;
  // 坏连接数量，记录检测到的无效连接数
  protected long badConnectionCount;

  public PoolState(PooledDataSource dataSource) {
    this.dataSource = dataSource;
  }

  /**
   * 获取请求总数
   *
   * @return 返回连接池被请求的总次数
   */
  public long getRequestCount() {
    lock.lock();
    try {
      return requestCount;
    } finally {
      lock.unlock();
    }
  }

  /**
   * 获取平均请求时间
   * <p>
   * 计算公式：累计请求时间 / 请求总数，如果请求总数为0则返回0
   *
   * @return 返回每个请求的平均耗时（毫秒）
   */
  public long getAverageRequestTime() {
    lock.lock();
    try {
      return requestCount == 0 ? 0 : accumulatedRequestTime / requestCount;
    } finally {
      lock.unlock();
    }
  }

  /**
   * 获取平均等待时间
   * <p>
   * 计算公式：累计等待时间 / 等待次数，如果等待次数为0则返回0
   *
   * @return 返回每次等待的平均耗时（毫秒）
   */
  public long getAverageWaitTime() {
    lock.lock();
    try {
      return hadToWaitCount == 0 ? 0 : accumulatedWaitTime / hadToWaitCount;
    } finally {
      lock.unlock();
    }
  }

  /**
   * 获取需要等待的次数
   * <p>
   * 记录连接池因没有可用连接而进入等待状态的次数
   *
   * @return 返回等待发生的总次数
   */
  public long getHadToWaitCount() {
    lock.lock();
    try {
      return hadToWaitCount;
    } finally {
      lock.unlock();
    }
  }

  /**
   * 获取坏连接的数量
   * <p>
   * 记录连接池检测到的无效连接总数
   *
   * @return 返回检测到的坏连接总数
   */
  public long getBadConnectionCount() {
    lock.lock();
    try {
      return badConnectionCount;
    } finally {
      lock.unlock();
    }
  }

  /**
   * 获取被声明为过期的连接数量
   * <p>
   * 记录因超时而被强制回收的连接总数
   *
   * @return 返回超时回收的连接总数
   */
  public long getClaimedOverdueConnectionCount() {
    lock.lock();
    try {
      return claimedOverdueConnectionCount;
    } finally {
      lock.unlock();
    }
  }

  /**
   * 获取平均过期连接检查时间
   * <p>
   * 计算公式：超时连接累计检出时间 / 超时连接数量，如果超时连接数为0则返回0
   *
   * @return 返回超时连接的平均检出时间（毫秒）
   */
  public long getAverageOverdueCheckoutTime() {
    lock.lock();
    try {
      return claimedOverdueConnectionCount == 0 ? 0
          : accumulatedCheckoutTimeOfOverdueConnections / claimedOverdueConnectionCount;
    } finally {
      lock.unlock();
    }
  }

  /**
   * 获取平均连接检查时间
   * <p>
   * 计算公式：累计检出时间 / 请求总数，如果请求总数为0则返回0
   *
   * @return 返回连接被检出使用的平均时间（毫秒）
   */
  public long getAverageCheckoutTime() {
    lock.lock();
    try {
      return requestCount == 0 ? 0 : accumulatedCheckoutTime / requestCount;
    } finally {
      lock.unlock();
    }
  }

  /**
   * 获取空闲连接数量
   * <p>
   * 返回当前在空闲连接列表中的连接数
   *
   * @return 返回空闲连接的总数量
   */
  public int getIdleConnectionCount() {
    lock.lock();
    try {
      return idleConnections.size();
    } finally {
      lock.unlock();
    }
  }

  /**
   * 获取活跃连接数量
   * <p>
   * 返回当前在活跃连接列表中的连接数
   *
   * @return 返回活跃连接的总数量
   */
  public int getActiveConnectionCount() {
    lock.lock();
    try {
      return activeConnections.size();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public String toString() {
    lock.lock();
    try {
      StringBuilder builder = new StringBuilder();
      builder.append("\n===CONFIGURATION==============================================");
      builder.append("\n jdbcDriver                     ").append(dataSource.getDriver());
      builder.append("\n jdbcUrl                        ").append(dataSource.getUrl());
      builder.append("\n jdbcUsername                   ").append(dataSource.getUsername());
      builder.append("\n jdbcPassword                   ")
          .append(dataSource.getPassword() == null ? "NULL" : "************");
      builder.append("\n poolMaxActiveConnections       ").append(dataSource.poolMaximumActiveConnections);
      builder.append("\n poolMaxIdleConnections         ").append(dataSource.poolMaximumIdleConnections);
      builder.append("\n poolMaxCheckoutTime            ").append(dataSource.poolMaximumCheckoutTime);
      builder.append("\n poolTimeToWait                 ").append(dataSource.poolTimeToWait);
      builder.append("\n poolPingEnabled                ").append(dataSource.poolPingEnabled);
      builder.append("\n poolPingQuery                  ").append(dataSource.poolPingQuery);
      builder.append("\n poolPingConnectionsNotUsedFor  ").append(dataSource.poolPingConnectionsNotUsedFor);
      builder.append("\n ---STATUS-----------------------------------------------------");
      builder.append("\n activeConnections              ").append(getActiveConnectionCount());
      builder.append("\n idleConnections                ").append(getIdleConnectionCount());
      builder.append("\n requestCount                   ").append(getRequestCount());
      builder.append("\n averageRequestTime             ").append(getAverageRequestTime());
      builder.append("\n averageCheckoutTime            ").append(getAverageCheckoutTime());
      builder.append("\n claimedOverdue                 ").append(getClaimedOverdueConnectionCount());
      builder.append("\n averageOverdueCheckoutTime     ").append(getAverageOverdueCheckoutTime());
      builder.append("\n hadToWait                      ").append(getHadToWaitCount());
      builder.append("\n averageWaitTime                ").append(getAverageWaitTime());
      builder.append("\n badConnectionCount             ").append(getBadConnectionCount());
      builder.append("\n===============================================================");
      return builder.toString();
    } finally {
      lock.unlock();
    }
  }
}
