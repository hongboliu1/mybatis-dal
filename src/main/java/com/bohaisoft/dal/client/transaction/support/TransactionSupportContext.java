package com.bohaisoft.dal.client.transaction.support;

import java.util.HashMap;
import java.util.Map;

public class TransactionSupportContext
{
  private static ThreadLocal<Map<String, Long>> transObj = new ThreadLocal<Map<String, Long>>();

  public static Map<String, Long> getTransactionObject() {
    Map<String, Long> obj = (Map<String, Long>)transObj.get();
    if (obj == null) {
      obj = new HashMap<String, Long>();
      transObj.set(obj);
    }
    return obj;
  }

  public static void clear() {
    transObj.remove();
  }
}