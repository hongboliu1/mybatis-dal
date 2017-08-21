package com.bohaisoft.dal.client.merger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.CollectionUtils;

public class ConcurrentSortMerger<E> implements IMerger<List<E>, List<E>>, InitializingBean,
        DisposableBean {

    private boolean         usingDefaultExecutor = false;

    private ExecutorService executor;
    private Comparator<E>   comparator;

    public List<E> merge(List<List<E>> entities) {
        List<E> resultList = new ArrayList<E>();
        if (!CollectionUtils.isEmpty(entities)) {
            if (entities.size() == 1) {
                resultList.addAll(entities.get(0));
            } else {
                List<List<E>> partialResult = new ArrayList<List<E>>();
                int pairs = entities.size() / 2;
                List<Future<List<E>>> futures = new ArrayList<Future<List<E>>>();

                for (int i = 0; i < pairs; i++) {
                    final List<E> llst = entities.get(i * 2);
                    final List<E> rlst = entities.get(i * 2 + 1);
                    futures.add(getExecutor().submit(new Callable<List<E>>() {
                        public List<E> call() throws Exception {
                            return partialSortMerge(llst, rlst);
                        }
                    }));
                }

                for (Future<List<E>> f : futures) {
                    try {
                        partialResult.add(f.get());
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    }
                }

                if (entities.size() % 2 == 1) {
                    partialResult.add(entities.get(pairs * 2));
                }

                resultList.addAll(merge(partialResult));
            }
        }
        return resultList;
    }

    protected List<E> partialSortMerge(List<E> llst, List<E> rlst) {
        List<E> resultList = new ArrayList<E>();
        int li = 0, ri = 0;
        while (li < llst.size() && ri < rlst.size()) {
            E le = llst.get(li);
            E re = rlst.get(ri);
            if (getComparator().compare(le, re) <= 0) {
                resultList.add(le);
                li++;
            } else {
                resultList.add(re);
                ri++;
            }
        }

        if (li < llst.size()) {
            resultList.addAll(llst.subList(li, llst.size()));
        }
        if (ri < rlst.size()) {
            resultList.addAll(rlst.subList(ri, rlst.size()));
        }
        return resultList;
    }

    public void setExecutor(ExecutorService executor) {
        this.executor = executor;
    }

    public ExecutorService getExecutor() {
        return executor;
    }

    public void afterPropertiesSet() throws Exception {
        if (getComparator() == null) {
            throw new IllegalArgumentException(
                    "you must provide a comparator for us to compare the element for merge.");
        }
        if (getExecutor() == null) {
            setExecutor(Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()));
            usingDefaultExecutor = true;
        }
    }

    public void destroy() throws Exception {
        if (usingDefaultExecutor) {
            getExecutor().shutdown();
        }
    }

    public void setComparator(Comparator<E> comparator) {
        this.comparator = comparator;
    }

    public Comparator<E> getComparator() {
        return comparator;
    }

}
