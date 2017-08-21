package com.bohaisoft.dal.client.merger;

import java.util.List;

public interface IMerger<T, R> {
    R merge(List<T> entities);
}
