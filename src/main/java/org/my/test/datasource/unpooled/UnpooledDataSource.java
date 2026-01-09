package org.my.test.datasource.unpooled;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.logging.Logger;
import javax.sql.DataSource;
import lombok.Data;

/**
 * 非池化数据源实现类，直接创建数据库连接而不使用连接池
 * 每次获取连接时都会创建新的物理连接，适用于轻量级应用场景
 *
 * @author aidan.liu
 * @version 1.0
 * @since 2026/1/9 13:53
 */
@Data
public class UnpooledDataSource implements DataSource {

  /**
   * 默认驱动类加载器
   */
  private ClassLoader driverClassLoader;
  /**
   * 默认驱动属性
   */
  private Properties driverProperties;
  /**
   * 驱动
   */
  private static final Map<String, Driver> registeredDrivers = new ConcurrentHashMap<>();

  private String driver;
  private String url;
  private String username;
  private String password;

  /**
   * 默认自动提交
   */
  private Boolean autoCommit;
  /**
   * 默认事务隔离级别
   */
  private Integer defaultTransactionIsolationLevel;
  /**
   * 默认网络超时时间
   */
  private Integer defaultNetworkTimeout;

  static {
    Enumeration<Driver> drivers = DriverManager.getDrivers();
    while (drivers.hasMoreElements()) {
      Driver driver = drivers.nextElement();
      registeredDrivers.put(driver.getClass().getName(), driver);
    }
  }

  public UnpooledDataSource() {
  }

  public UnpooledDataSource(String driver, String url, String username, String password) {
    this.driver = driver;
    this.url = url;
    this.username = username;
    this.password = password;
  }

  public UnpooledDataSource(String driver, String url, Properties driverProperties) {
    this.driver = driver;
    this.url = url;
    this.driverProperties = driverProperties;
  }

  public UnpooledDataSource(ClassLoader driverClassLoader, String driver, String url, String username,
                            String password) {
    this.driverClassLoader = driverClassLoader;
    this.driver = driver;
    this.url = url;
    this.username = username;
    this.password = password;
  }

  public UnpooledDataSource(ClassLoader driverClassLoader, String driver, String url, Properties driverProperties) {
    this.driverClassLoader = driverClassLoader;
    this.driver = driver;
    this.url = url;
    this.driverProperties = driverProperties;
  }

  @Override
  public Connection getConnection() throws SQLException {
    return doGetConnection(username, password);
  }

  @Override
  public Connection getConnection(String username, String password) throws SQLException {
    return doGetConnection(username, password);
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

  private Connection doGetConnection(String username, String password) throws SQLException {
    Properties props = new Properties();
    if (driverProperties != null) {
      props.putAll(driverProperties);
    }
    if (username != null) {
      props.setProperty("user", username);
    }
    if (password != null) {
      props.setProperty("password", password);
    }
    return doGetConnection(props);
  }

  private Connection doGetConnection(Properties properties) throws SQLException {
    initializeDriver();
    Connection connection = DriverManager.getConnection(url, properties);
    configureConnection(connection);
    return connection;
  }

  /**
   * 初始化数据库驱动程序
   * <p>
   * 该方法负责加载并注册指定的数据库驱动程序。如果驱动程序尚未注册，
   * 则通过反射创建驱动实例，并使用DriverProxy进行包装后注册到DriverManager中。
   * 驱动程序的加载会优先使用指定的类加载器，如果未指定则使用默认的Resources类加载器。
   *
   * @throws SQLException 如果驱动程序设置过程中发生错误，则抛出SQLException异常
   */
  private void initializeDriver() throws SQLException {
    try {
      // 使用computeIfAbsent确保驱动程序只被注册一次，避免重复注册
      registeredDrivers.computeIfAbsent(driver, x -> {
        Class<?> driverType;
        try {
          if (driverClassLoader != null) {
            driverType = Class.forName(x, true, driverClassLoader);
          } else {
            driverType = Resources.classForName(x);
          }
          Driver driverInstance = (Driver) driverType.getDeclaredConstructor().newInstance();
          // 创建DriverProxy包装实例并注册到DriverManager中
          DriverManager.registerDriver(new DriverProxy(driverInstance));
          return driverInstance;
        } catch (Exception e) {
          throw new RuntimeException("Error setting driver on UnpooledDataSource.", e);
        }
      });
    } catch (RuntimeException re) {
      throw new SQLException("Error setting driver on UnpooledDataSource.", re.getCause());
    }
  }

  /**
   * 配置数据库连接的各种属性
   *
   * @param conn 需要配置的数据库连接对象
   * @throws SQLException 当设置连接属性失败时抛出
   */
  private void configureConnection(Connection conn) throws SQLException {
    // 设置网络超时时间
    if (defaultNetworkTimeout != null) {
      conn.setNetworkTimeout(Executors.newSingleThreadExecutor(), defaultNetworkTimeout);
    }
    // 设置自动提交模式
    if (autoCommit != null && autoCommit != conn.getAutoCommit()) {
      conn.setAutoCommit(autoCommit);
    }
    // 设置事务隔离级别
    if (defaultTransactionIsolationLevel != null) {
      conn.setTransactionIsolation(defaultTransactionIsolationLevel);
    }
  }


  private static class DriverProxy implements Driver {
    private final Driver driver;

    DriverProxy(Driver d) {
      this.driver = d;
    }

    @Override
    public boolean acceptsURL(String u) throws SQLException {
      return this.driver.acceptsURL(u);
    }

    @Override
    public Connection connect(String u, Properties p) throws SQLException {
      return this.driver.connect(u, p);
    }

    @Override
    public int getMajorVersion() {
      return this.driver.getMajorVersion();
    }

    @Override
    public int getMinorVersion() {
      return this.driver.getMinorVersion();
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String u, Properties p) throws SQLException {
      return this.driver.getPropertyInfo(u, p);
    }

    @Override
    public boolean jdbcCompliant() {
      return this.driver.jdbcCompliant();
    }

    @Override
    public Logger getParentLogger() {
      return Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    }
  }
}
