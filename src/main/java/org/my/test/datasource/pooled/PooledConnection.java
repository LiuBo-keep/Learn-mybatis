package org.my.test.datasource.pooled;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import lombok.Data;
import org.my.test.reflection.ExceptionUtil;

/**
 * 连接池连接类，实现了InvocationHandler接口，用于代理数据库连接
 * <p>
 * 该类是数据库连接池的核心组件，包装了真实的数据库连接对象，通过动态代理机制
 * 拦截对数据库连接的操作，实现连接的管理和复用。当应用程序调用close方法时，
 * 不会真正关闭物理连接，而是将其归还到连接池中供后续使用。
 * <p>
 * 主要功能包括：
 * - 连接状态管理（有效/无效）
 * - 连接生命周期跟踪（创建时间、检出时间、最后使用时间）
 * - 动态代理拦截连接方法调用
 * - 连接有效性验证
 * - 连接池归还机制
 *
 * @author aidan.liu
 * @version 1.0
 * @since 2026/1/9 14:12
 */
@Data
public class PooledConnection implements InvocationHandler {

  // 关闭方法的字符串常量，用于拦截close方法调用
  private static final String CLOSE = "close";
  // 代理连接实现的接口数组，仅包含Connection接口
  private static final Class<?>[] IFACES = {Connection.class};

  // 连接对象的hashcode值，用于标识连接
  private final int hashCode;
  // 关联的连接池数据源对象，用于连接归还等操作
  private final PooledDataSource dataSource;
  // 真实的数据库连接对象
  private final Connection realConnection;
  // 代理连接对象，对外提供Connection接口的代理实现
  private final Connection proxyConnection;
  // 连接被检出的时间戳（从连接池获取的时间）
  private long checkoutTimestamp;
  // 连接创建的时间戳
  private long createdTimestamp;
  // 连接最后使用的时间戳
  private long lastUsedTimestamp;
  // 连接类型编码，用于验证连接类型的一致性
  private int connectionTypeCode;
  // 连接有效性标志，true表示连接有效，false表示连接已失效
  private boolean valid;

  public PooledConnection(Connection connection, PooledDataSource dataSource) {
    this.hashCode = connection.hashCode();
    this.realConnection = connection;
    this.dataSource = dataSource;
    this.createdTimestamp = System.currentTimeMillis();
    this.lastUsedTimestamp = System.currentTimeMillis();
    this.valid = true;
    this.proxyConnection = (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), IFACES, this);
  }


  /**
   * 使连接失效，标记该连接不可用
   * <p>
   * 将连接的有效性标志设置为false，此后该连接将无法被使用
   * 通常在连接出现异常或连接池关闭时调用
   */
  public void invalidate() {
    valid = false;
  }

  /**
   * 检查连接是否有效
   * <p>
   * 验证连接的有效性需要同时满足三个条件：
   * 1. 连接对象本身标记为有效
   * 2. 真实连接对象不为null
   * 3. 通过数据源的ping连接验证
   *
   * @return 如果连接有效且数据源ping成功则返回true，否则返回false
   */
  public boolean isValid() {
    return valid && realConnection != null && dataSource.pingConnection(this);
  }

  /**
   * 获取真实连接的hashcode
   * <p>
   * 返回被包装的真实数据库连接对象的hashcode值
   * 如果真实连接为null，则返回0
   *
   * @return 真实连接的hashcode，如果真实连接为null则返回0
   */
  public int getRealHashCode() {
    return realConnection == null ? 0 : realConnection.hashCode();
  }

  /**
   * 获取自上次使用以来经过的时间
   * <p>
   * 计算从最后使用时间到当前时间的毫秒数差值
   * 用于连接池的心跳检测和连接回收策略
   *
   * @return 毫秒数，表示自上次使用以来的时间间隔
   */
  public long getTimeElapsedSinceLastUse() {
    return System.currentTimeMillis() - lastUsedTimestamp;
  }

  /**
   * 获取连接的年龄（从创建到现在的时间）
   * <p>
   * 计算从连接创建时间到当前时间的毫秒数差值
   * 用于连接池的连接生命周期管理和过期检测
   *
   * @return 毫秒数，表示连接从创建到现在的时间间隔
   */
  public long getAge() {
    return System.currentTimeMillis() - createdTimestamp;
  }

  /**
   * 获取连接被检出的时间（从检出到现在的时间）
   * <p>
   * 计算从连接被检出时间到当前时间的毫秒数差值
   * 用于检测连接是否超时未归还，实现超时回收机制
   *
   * @return 毫秒数，表示连接从检出到现在的时间间隔
   */
  public long getCheckoutTime() {
    return System.currentTimeMillis() - checkoutTimestamp;
  }

  /**
   * 比较两个PooledConnection对象是否相等
   * <p>
   * 支持与PooledConnection对象和Connection对象的比较
   * 与PooledConnection比较时比较真实连接的hashcode
   * 与Connection比较时比较连接的hashcode
   *
   * @param obj 要比较的对象
   * @return 如果对象相等则返回true，否则返回false
   */
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof PooledConnection) {
      return realConnection.hashCode() == ((PooledConnection) obj).realConnection.hashCode();
    }
    if (obj instanceof Connection) {
      return hashCode == obj.hashCode();
    } else {
      return false;
    }
  }

  /**
   * 代理方法调用处理
   * <p>
   * 拦截对Connection接口方法的调用，特殊处理close方法
   * 对于close方法调用，将其重定向到连接池的归还操作
   * 对于其他方法调用，先检查连接有效性再委托给真实连接
   *
   * @param proxy  代理对象
   * @param method 被调用的方法
   * @param args   方法参数
   * @return 方法执行结果
   * @throws Throwable 方法执行异常
   */
  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    String methodName = method.getName();
    if (CLOSE.equals(methodName)) {
      // 当调用close方法时，将连接归还到连接池而不是真正关闭
      dataSource.pushConnection(this);
      return null;
    }
    try {
      if (!Object.class.equals(method.getDeclaringClass())) {
        // issue #579 toString() should never fail
        // throw an SQLException instead of a Runtime
        checkConnection();
      }
      return method.invoke(realConnection, args);
    } catch (Throwable t) {
      throw ExceptionUtil.unwrapThrowable(t);
    }
  }

  /**
   * 检查连接状态，如果连接无效则抛出异常
   * <p>
   * 在执行数据库操作前验证连接的有效性
   * 如果连接已失效，抛出SQLException异常
   *
   * @throws SQLException 当连接无效时抛出此异常
   */
  private void checkConnection() throws SQLException {
    if (!valid) {
      throw new SQLException("Error accessing PooledConnection. Connection is invalid.");
    }
  }
}
