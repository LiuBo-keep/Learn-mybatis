package org.my.test.datasource;

import java.util.Properties;
import javax.sql.DataSource;

/**
 * @author aidan.liu
 * @version 1.0
 * @since 2026/1/9 13:51
 */
public interface DataSourceFactory {

  void setProperties(Properties props);

  DataSource getDataSource();
}
