package io.agora.cruise.parser.util;

import java.util.concurrent.locks.Lock;

/** LockUtils. */
public class LockUtils {

    /**
     * lock function.
     *
     * @param functionHandler functionHandler
     * @param lock lock
     * @param <T> type
     * @return function result.
     */
    public static <T> T lock(FunctionHandler<T> functionHandler, Lock lock) {
        lock.lock();
        try {
            return functionHandler.apply();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
    }

    /**
     * FunctionHandler.
     *
     * @param <T>
     */
    public interface FunctionHandler<T> {

        /**
         * invoke function.
         *
         * @return result
         */
        T apply() throws Exception;
    }
}
