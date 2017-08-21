package com.bohaisoft.dal.client.exception;

import org.springframework.dao.DataAccessException;

public class RoutingException extends DataAccessException {
    private static final long serialVersionUID = -5001927974502714777L;

    public RoutingException(String msg) {
        super(msg);
    }
        public RoutingException(String msg,Throwable e) {
        super(msg,e);
    }
}