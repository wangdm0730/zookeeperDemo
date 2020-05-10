package com.lagou.edu.rpc.common.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 用来标识该类的接口已经暴露
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface RpcService {

    /**
     * 服务版本号，待实现
     *
     * @return
     */
    String version() default "";

    /**
     * 超时时间，待实现
     *
     * @return
     */
    int timeout() default -1;
}