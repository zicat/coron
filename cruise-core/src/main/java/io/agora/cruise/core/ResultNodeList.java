package io.agora.cruise.core;

import java.util.ArrayList;

/**
 * NotNullList.
 *
 * @param <T>
 */
public class ResultNodeList<T> extends ArrayList<ResultNode<T>> {

    public ResultNodeList() {
        super();
    }

    public ResultNodeList(ResultNode<T> e) {
        super();
        add(e);
    }

    @Override
    public boolean add(ResultNode<T> e) {
        if (e == null || e.isEmpty()) {
            return false;
        }
        return super.add(e);
    }
}
