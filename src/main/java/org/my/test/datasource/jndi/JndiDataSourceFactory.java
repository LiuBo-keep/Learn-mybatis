package org.my.test.datasource.jndi;

import java.util.Map;
import java.util.Properties;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import org.my.test.datasource.DataSourceException;
import org.my.test.datasource.DataSourceFactory;

/**
 * JNDI数据源工厂实现类，用于通过JNDI查找获取数据源
 * <p>
 * 该类实现了数据源工厂接口，通过JNDI（Java Naming and Directory Interface）机制
 * 从应用服务器的命名服务中查找和获取数据源对象。支持通过环境属性配置InitialContext，
 * 并支持在指定的初始上下文或直接在根上下文中查找数据源。
 * <p>
 * 配置属性说明：
 * - initial_context: 指定初始上下文名称，可选
 * - data_source: 指定数据源名称，必需
 * - env.*: 以env.前缀开头的属性用于配置InitialContext环境
 *
 * @author aidan.liu
 * @version 1.0
 * @since 2026/1/9 15:07
 */
public class JndiDataSourceFactory implements DataSourceFactory {

  // JNDI查找相关的常量定义
  public static final String INITIAL_CONTEXT = "initial_context";  // 初始上下文属性名
  public static final String DATA_SOURCE = "data_source";          // 数据源属性名
  public static final String ENV_PREFIX = "env.";                  // 环境属性前缀

  // 存储通过JNDI查找到的数据源对象
  private DataSource dataSource;

  /**
   * 设置属性并初始化数据源
   * <p>
   * 根据提供的配置属性创建InitialContext，然后通过JNDI查找获取数据源对象。
   * 支持两种查找方式：
   * 1. 如果同时配置了initial_context和data_source，则先查找初始上下文，再在该上下文中查找数据源
   * 2. 如果只配置了data_source，则直接在根上下文中查找数据源
   * <p>
   * 该方法会提取以env.前缀开头的属性作为InitialContext的环境配置。
   *
   * @param properties 配置属性集合，包含JNDI相关配置信息
   * @throws DataSourceException 当JNDI查找失败或配置出现错误时抛出此异常
   */
  @Override
  public void setProperties(Properties properties) {
    try {
      InitialContext initCtx;
      // 获取环境属性配置
      Properties env = getEnvProperties(properties);
      if (env == null) {
        initCtx = new InitialContext();
      } else {
        initCtx = new InitialContext(env);
      }

      // 根据配置属性查找数据源
      if (properties.containsKey(INITIAL_CONTEXT) && properties.containsKey(DATA_SOURCE)) {
        // 在指定的初始上下文中查找数据源
        Context ctx = (Context) initCtx.lookup(properties.getProperty(INITIAL_CONTEXT));
        dataSource = (DataSource) ctx.lookup(properties.getProperty(DATA_SOURCE));
      } else if (properties.containsKey(DATA_SOURCE)) {
        // 在根上下文中直接查找数据源
        dataSource = (DataSource) initCtx.lookup(properties.getProperty(DATA_SOURCE));
      }

    } catch (NamingException e) {
      throw new DataSourceException("There was an error configuring JndiDataSourceTransactionPool. Cause: " + e, e);
    }
  }

  /**
   * 获取已配置的数据源
   * <p>
   * 返回通过setProperties方法配置和查找得到的数据源对象。
   * 只有在setProperties方法成功执行后，该方法才会返回有效的数据源对象。
   *
   * @return 数据源对象，如果尚未配置或配置失败则返回null
   */
  @Override
  public DataSource getDataSource() {
    return dataSource;
  }

  /**
   * 从所有属性中提取以ENV_PREFIX开头的环境属性
   * <p>
   * 该方法遍历所有属性，识别以env.前缀开头的属性，并将这些属性作为
   * InitialContext的环境配置参数。提取时会移除env.前缀，只保留实际的环境属性名。
   *
   * @param allProps 包含所有属性的Properties集合
   * @return 提取的环境属性Properties对象，如果没有匹配的属性则返回null
   */
  private static Properties getEnvProperties(Properties allProps) {
    Properties contextProperties = null;
    for (Map.Entry<Object, Object> entry : allProps.entrySet()) {
      String key = (String) entry.getKey();
      String value = (String) entry.getValue();
      if (key.startsWith(ENV_PREFIX)) {
        if (contextProperties == null) {
          contextProperties = new Properties();
        }
        // 移除env.前缀，保留实际的环境属性名
        contextProperties.put(key.substring(ENV_PREFIX.length()), value);
      }
    }
    return contextProperties;
  }
}
