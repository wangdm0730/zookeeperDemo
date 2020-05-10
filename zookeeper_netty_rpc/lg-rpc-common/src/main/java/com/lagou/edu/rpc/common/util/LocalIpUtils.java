/**
 * @Description:
 */
package com.lagou.edu.rpc.common.util;


import org.springframework.util.StringUtils;

import java.net.*;
import java.util.Enumeration;

/**
 * 获取本机IP工具类
 */
public class LocalIpUtils {

    /**
     * 获取本机内网地址
     **/
    public static String getLocalIpAddr() {
        InetAddress ip = null;
        if (isWindows()) {
            try {
                ip = InetAddress.getLocalHost();
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
            if (ip != null) {
                return ip.getHostAddress();
            }
        }
        String ipAddress = getLocalIp(true);
        if (StringUtils.isEmpty(ipAddress)) {//如果eth0网卡为空
            ipAddress = getLocalIp(false);
        }
        return ipAddress;
    }

    /**
     * 获取本地地址
     *
     * @param justEth0 true - 只看eth0  false  所有
     * @return
     * @Date:2014-4-24
     * @Author:Guibin Zhang
     * @Description:
     */
    private static String getLocalIp(boolean justEth0) {
        InetAddress ip = null;
        Enumeration<NetworkInterface> allNetInterfaces = null;
        try {
            allNetInterfaces = NetworkInterface.getNetworkInterfaces();
        } catch (SocketException e) {
            e.printStackTrace();
        }
        while (allNetInterfaces.hasMoreElements()) {
            NetworkInterface netInterface = (NetworkInterface) allNetInterfaces.nextElement();
            if (!justEth0 || "eth0".equalsIgnoreCase(netInterface.getName())) {
                Enumeration<InetAddress> addresses = netInterface.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    ip = (InetAddress) addresses.nextElement();
                    if (ip != null && ip.getHostAddress() != null && ip instanceof Inet4Address && ip.getHostAddress().indexOf(".") != -1
                            && !ip.getHostAddress().startsWith("192.168.") && !"127.0.0.1".equals(ip.getHostAddress()) && !"localhost".equals(ip.getHostAddress())) {
                        return ip.getHostAddress();
                    }
                }
            }
        }
        return null;
    }

    /**
     * 判断系统是不是windows
     **/
    public static boolean isWindows() {
        String osName = System.getProperty("os.name");
        if (osName.toLowerCase().indexOf("windows") > -1) {
            return true;
        }
        return false;
    }

    /**
     * 判断系统是不是mac
     **/
    public static boolean isMac() {
        String osName = System.getProperty("os.name");
        if (osName.toLowerCase().indexOf("mac") > -1) {
            return true;
        }
        return false;
    }

    /**
     * 判断系统是不是linux
     **/
    public static boolean isLinux() {
        String osName = System.getProperty("os.name");
        if (osName.toLowerCase().indexOf("linux") > -1) {
            return true;
        }
        return false;
    }
}
