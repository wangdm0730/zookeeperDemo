package com.lagou.config;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.RetryNTimes;

import javax.sql.DataSource;
import java.beans.PropertyVetoException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type.CHILD_UPDATED;

/**
 * 利用Zookeeper来构建数据源，并且在变更时动态更改配置<br />
 * 1. 基于{@link DataSource}方便在其他框架中使用，提高通用性(例如SpringBean托管)<br />
 * 2. 利用代理模式，来交给第三方的数据源{@link ComboPooledDataSource}来进行处理，这里只负责动态变更<br />
 * 3. 当检测到Zookeeper中的配置变更时，动态更改数据源对象的引用来达到切换的目的, 并且将原先的数据源关闭(防止占用不必要链接)
 */
public class DynamicConfigDatasource implements DataSource {

    /**
     * 当前的配置信息
     */
    private Map<String, String> configMap = new HashMap<>();

    /**
     * 需要监听的配置路径
     */
    private static final String CONFIG_PREFIX = "/datasource";

    /**
     * 与Zookeeper建立链接的client
     */
    private final CuratorFramework zkClient;

    /**
     * 真实所代理的连接池信息
     */
    private volatile ComboPooledDataSource datasource;

    /**
     * 新建一个数据源
     * @param zookeeperAddress zookeeper访问地址
     */
    public static DynamicConfigDatasource create(String zookeeperAddress) {
        return new DynamicConfigDatasource(CuratorFrameworkFactory.newClient(zookeeperAddress, new RetryNTimes(3, 1000)));
    }

    /**
     * 根据Zookeeper链接来生成
     */
    private DynamicConfigDatasource(CuratorFramework zkClient) {
        this.zkClient = zkClient;
        if (!zkClient.getState().equals(CuratorFrameworkState.STARTED)) {
            zkClient.start();
        }

        try {
            // 保存当前的数据源配置信息
            saveDatasourceConfig();

            // 进行初始化真实的数据源
            initDataSource();

            // 进行启动监听
            startListener();
        } catch (Exception e) {
            throw new IllegalStateException("启动动态代理数据源失败！", e);
        }
    }

    @Override
    public Connection getConnection() throws SQLException {
        return datasource.getConnection();
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return datasource.getConnection(username, password);
    }

    /**
     * 获取zk配置, 并且将其保存到内存变量中
     */
    private void saveDatasourceConfig() throws Exception {
        List<String> childrenNames = zkClient.getChildren().forPath(CONFIG_PREFIX);
        for (String childrenName : childrenNames) {
            String value = new String(zkClient.getData().forPath(CONFIG_PREFIX + "/" + childrenName));
            configMap.put(childrenName,value);
        }
    }

    /**
     * 启动对数据源的监听
     * @throws Exception
     */
    private void startListener() throws Exception {

        PathChildrenCache watcher = new PathChildrenCache(zkClient, CONFIG_PREFIX, true);
        watcher.getListenable().addListener(new PathChildrenCacheListener() {

            public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent event) throws Exception {
                if (event.getType() != CHILD_UPDATED) {
                    return;
                }
                System.out.println("检测到数据源信息变更:" + new String(event.getData().getData()));

                // 重置配置信息
                saveDatasourceConfig();

                // 获取当前系统中的数据源
                final ComboPooledDataSource currentDatasource = DynamicConfigDatasource.this.datasource;

                // 进行重置数据源
                initDataSource();

                // 关闭系统中遗留的数据源信息
                if (currentDatasource != null) {
                    currentDatasource.close();
                }
            }
        });
        watcher.start();
        System.out.println("完成对数据源的监听操作");
    }

    /**
     * 从配置文件中初始化数据源
     * @throws PropertyVetoException
     * @throws SQLException
     */
    private void initDataSource() throws PropertyVetoException, SQLException {
        ComboPooledDataSource dataSource = new ComboPooledDataSource();
        dataSource.setDriverClass(configMap.get("driverClassName"));
        dataSource.setJdbcUrl(configMap.get("dbJDBCUrl"));
        dataSource.setUser(configMap.get("username"));
        dataSource.setPassword(configMap.get("password"));

        // 将数据源切换为新的数据源
        this.datasource = dataSource;
    }


    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return null;
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return 0;
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }

}
