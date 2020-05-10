package com.lagou.edu.rpc.registry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 读取包下面的类
 */
public class ProviderLoader {
    private static final Logger logger = LoggerFactory.getLogger(ProviderLoader.class);

    // 用来装服务提供者的实例
    private static Map<String, Object> instanceCacheMap = new ConcurrentHashMap<>();

    // 用来存放实现类的类名
    private static List<String> providerClassList = new ArrayList<>();

    static {
        loadProviderInstance("com.lagou.edu");
    }

    /**
     * 扫描provider包下面的实现类，并放进缓存instanceMap里面
     *
     * @param packageName
     */
    private static void loadProviderInstance(String packageName) {
        findProviderClass(packageName);
        putProviderInstance(packageName);
    }

    /**
     * 找到provider包下所有的实现类名，并放进providerClassList里
     */
    private static void findProviderClass(final String packageName) {
        // 所以得用匿名内部类来解决
        // 这里由classLoader的getResource方法获得包名并封装成URL形式
        URL url = new Object() {
            public URL getPath() {
                String packageDir = packageName.replace(".", "/");
                URL o = this.getClass().getClassLoader().getResource(packageDir);
                return o;
            }

        }.getPath();
        // 将该包名转换为File格式，用于以下判断是文件夹还是文件，若是文件夹则递归调用本方法，
        // 若不是文件夹则直接将该provider的实现类的名字放到providerClassList中
        File dir = new File(url.getFile());
        File[] fileArr = dir.listFiles();
        for (File file : fileArr) {
            if (file.isDirectory()) {
                findProviderClass(packageName + "." + file.getName());
            } else {
                providerClassList.add(packageName + "." + file.getName().replace(".class", ""));
            }
        }

    }

    /**
     * 遍历providerClassList集合的实现类，并依次将实现类的接口作为key，实现类的实例作为值放入instanceCacheMap集合中,其实这里也是模拟服务注册的过程
     * 注意这里没有处理一个接口有多个实现类的情况
     */
    private static void putProviderInstance(String packageName) {
        for (String providerClassName : providerClassList) {
            // 已经得到providerClassName,因此可以通过反射来生成实例
            try {
                Class<?> providerClass = Class.forName(providerClassName);
                Class<?>[] interfaces = providerClass.getInterfaces();
                if (null == interfaces || interfaces.length <= 0) {
                    continue;
                }
                // 这里得到实现类的接口的全限定名作为key，因为consumer调用时是传接口的全限定名过来从缓存中获取实例再进行反射调用
                String providerClassInterfaceName = interfaces[0].getName();
                if (!providerClassInterfaceName.startsWith(packageName)) {
                    continue;
                }
                // 得到Provicder实现类的实例
                Object instance = providerClass.newInstance();
                instanceCacheMap.put(providerClassInterfaceName, instance);
                logger.info("注册了" + providerClassInterfaceName + "的服务");
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }

    public static Map<String, Object> getInstanceCacheMap() {
        return instanceCacheMap;
    }
}
