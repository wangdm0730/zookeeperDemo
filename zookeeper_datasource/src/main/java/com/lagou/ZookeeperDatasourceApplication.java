package com.lagou;

import com.lagou.config.DynamicConfigDatasource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ZookeeperDatasourceApplication {

    public static void main(String[] args) throws Exception {
        final DynamicConfigDatasource datasource = DynamicConfigDatasource.create("192.168.159.158:2181");
        System.out.println("初始化数据域完成！");
        testConnection(datasource);

        // 不停当代用户输入，这期间用来等待数据变更
        while (true) {
            final int read = System.in.read();
            testConnection(datasource);
        }
    }

    private static void testConnection(DynamicConfigDatasource datasource) throws SQLException {
        try (
            Connection connection = datasource.getConnection();
            ResultSet resultSet = connection.prepareStatement("select user()").executeQuery();
        ) {
            while (resultSet.next()) {
                System.out.println("当前登录的用户名:" + resultSet.getString(1));
            }
        }
    }

}
