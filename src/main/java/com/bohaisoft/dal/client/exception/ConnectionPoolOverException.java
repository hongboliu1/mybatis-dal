package com.bohaisoft.dal.client.exception;

import org.springframework.dao.DataAccessException;

public class ConnectionPoolOverException extends DataAccessException {
    private static final long serialVersionUID = -5001927974502714777L;

    public ConnectionPoolOverException(String msg) {
        super(msg);
    }
}