package com.bohaisoft.dal.client.transaction.support;


/**
 * Created by IntelliJ IDEA.
 * User: zhanghonglun
 * Date: 2011-10-27
 * Time: 15:09:10
 * To change this template use File | Settings | File Templates.
 */
public class TransactionContext {
	
    private static ThreadLocal<TransactionObject> transObj = new ThreadLocal<>();//数据库事务对象

    public static TransactionObject getTransactionObject() {
        return transObj.get();
    }
    
    public static void setTransactionObject(TransactionObject txObject) {
    	transObj.set(txObject);
    }

    public static void clear() {
    	transObj.remove();
    }
}
