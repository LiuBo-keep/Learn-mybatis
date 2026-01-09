package org.my.test.datasource.unpooled;

import java.util.Properties;
import javax.sql.DataSource;
import org.my.test.datasource.DataSourceFactory;

/**
 * 非池化数据源工厂类
 * 实现DataSourceFactory接口，用于创建不使用连接池的BasicDataSource数据源
 * 每次获取连接时都会创建新的数据库连接，适用于简单场景或测试环境
 *
 * @author aidan.liu
 * @version 1.0
 * @since 2026/1/9 14:05
 */
public class UnpooledDataSourceFactory implements DataSourceFactory {

  private static final String DRIVER_PROPERTY_PREFIX = "driver.";
  private static final int DRIVER_PROPERTY_PREFIX_LENGTH = DRIVER_PROPERTY_PREFIX.length();

  protected DataSource dataSource;

  public UnpooledDataSourceFactory() {
    this.dataSource = new UnpooledDataSource();
  }

  /**
   * 设置数据源属性
   * 该方法处理传入的属性集合，将带有驱动前缀的属性设置到驱动属性中，
   * 其他属性则直接设置到数据源对象中
   *
   * @param properties 包含数据源配置属性的Properties对象，其中可能包含驱动属性和其他数据源属性
   */
  public void setProperties(Properties properties) {
    Properties driverProperties = new Properties();
    MetaObject metaDataSource = SystemMetaObject.forObject(dataSource);

    // 遍历所有属性键值对，区分处理驱动属性和其他数据源属性
    for (Object key : properties.keySet()) {
      String propertyName = (String) key;
      if (propertyName.startsWith(DRIVER_PROPERTY_PREFIX)) {
        String value = properties.getProperty(propertyName);
        driverProperties.setProperty(propertyName.substring(DRIVER_PROPERTY_PREFIX_LENGTH), value);
      } else if (metaDataSource.hasSetter(propertyName)) {
        String value = (String) properties.get(propertyName);
        Object convertedValue = convertValue(metaDataSource, propertyName, value);
        metaDataSource.setValue(propertyName, convertedValue);
      } else {
        throw new DataSourceException("Unknown DataSource property: " + propertyName);
      }
    }

    // 如果存在驱动属性，则统一设置到数据源的driverProperties属性中
    if (driverProperties.size() > 0) {
      metaDataSource.setValue("driverProperties", driverProperties);
    }
  }


  @Override
  public DataSource getDataSource() {
    return dataSource;
  }

  /**
   * 将字符串值转换为指定属性类型的目标值
   *
   * @param metaDataSource 元数据对象，用于获取属性的类型信息
   * @param propertyName   属性名称，用于确定目标类型
   * @param value          待转换的字符串值
   * @return 转换后的目标类型对象，如果目标类型不是Integer、Long或Boolean，则返回原字符串值
   */
  private Object convertValue(MetaObject metaDataSource, String propertyName, String value) {
    Object convertedValue = value;
    Class<?> targetType = metaDataSource.getSetterType(propertyName);

    // 根据目标类型进行相应的字符串转换
    if (targetType == Integer.class || targetType == int.class) {
      convertedValue = Integer.valueOf(value);
    } else if (targetType == Long.class || targetType == long.class) {
      convertedValue = Long.valueOf(value);
    } else if (targetType == Boolean.class || targetType == boolean.class) {
      convertedValue = Boolean.valueOf(value);
    }
    return convertedValue;
  }
}
