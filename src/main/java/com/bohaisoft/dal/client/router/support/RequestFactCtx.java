package com.bohaisoft.dal.client.router.support;

/**
 * Created by IntelliJ IDEA.
 * User: Administrator
 * Date: 12-2-24
 * Time: 下午1:10
 * To change this template use File | Settings | File Templates.
 */
public class RequestFactCtx {
    private static ThreadLocal<RequestFactCtxVO> rfvoholder = new ThreadLocal<RequestFactCtxVO>();//私有静态变量

    public static RequestFactCtxVO getRfvoholder(){
        return rfvoholder.get();
    }

    public static void setRfvoholder(RequestFactCtxVO rfvo){
        rfvoholder.set(rfvo);
    }

    public static void clearRfvoholder(){
        rfvoholder.remove();
    }
}
