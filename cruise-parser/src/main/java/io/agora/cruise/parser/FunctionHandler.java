package io.agora.cruise.parser;

/** FunctionHandler. */
public interface FunctionHandler<T> {

    /**
     * invoke function.
     *
     * @return
     */
    T apply() throws Exception;
}
