package com.lingyun.business.common.util;

import org.slf4j.Logger;

/**
 * 异常处理工具类
 * 提供统一的异常处理和日志记录方法
 *
 * @author wxx
 */
public class ExceptionUtil {

    private ExceptionUtil() {
        // 工具类禁止实例化
    }

    /**
     * 安全执行操作，捕获异常并记录日志
     *
     * @param logger   日志对象
     * @param action   要执行的操作
     * @param errorMsg 错误消息
     */
    public static void safeExecute(Logger logger, Runnable action, String errorMsg) {
        try {
            action.run();
        } catch (Exception e) {
            logger.error("{}: {}", errorMsg, e.getMessage(), e);
        }
    }

    /**
     * 安全执行操作，捕获异常并记录日志，返回默认值
     *
     * @param logger       日志对象
     * @param supplier     要执行的操作
     * @param defaultValue 异常时的默认值
     * @param errorMsg     错误消息
     * @param <T>          返回类型
     * @return 操作结果或默认值
     */
    public static <T> T safeExecute(Logger logger, java.util.function.Supplier<T> supplier,
                                    T defaultValue, String errorMsg) {
        try {
            return supplier.get();
        } catch (Exception e) {
            logger.error("{}: {}", errorMsg, e.getMessage(), e);
            return defaultValue;
        }
    }

    /**
     * 安全执行操作，仅记录debug级别日志
     *
     * @param logger   日志对象
     * @param action   要执行的操作
     * @param debugMsg 调试消息
     */
    public static void safeExecuteDebug(Logger logger, Runnable action, String debugMsg) {
        try {
            action.run();
        } catch (Exception e) {
            logger.debug("{}: {}", debugMsg, e.getMessage());
        }
    }

    /**
     * 安全执行操作，捕获异常返回null
     *
     * @param supplier 要执行的操作
     * @param <T>      返回类型
     * @return 操作结果或null
     */
    public static <T> T safeGet(java.util.function.Supplier<T> supplier) {
        try {
            return supplier.get();
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 包装受检异常为运行时异常
     *
     * @param e       原始异常
     * @param message 错误消息
     * @return 运行时异常
     */
    public static RuntimeException wrapException(Exception e, String message) {
        return new RuntimeException(message + ": " + e.getMessage(), e);
    }

    /**
     * 获取异常的根本原因
     *
     * @param throwable 异常
     * @return 根本原因
     */
    public static Throwable getRootCause(Throwable throwable) {
        Throwable cause = throwable;
        while (cause.getCause() != null && cause.getCause() != cause) {
            cause = cause.getCause();
        }
        return cause;
    }

    /**
     * 获取异常的完整堆栈信息（用于日志）
     *
     * @param throwable 异常
     * @return 堆栈信息字符串
     */
    public static String getStackTraceString(Throwable throwable) {
        if (throwable == null) {
            return "";
        }
        java.io.StringWriter sw = new java.io.StringWriter();
        java.io.PrintWriter pw = new java.io.PrintWriter(sw);
        throwable.printStackTrace(pw);
        return sw.toString();
    }
}
