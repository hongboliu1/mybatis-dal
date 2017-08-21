package com.bohaisoft.dal.client.exception;

import org.springframework.dao.DataAccessException;

public class NoDatasourceFoundException extends DataAccessException {
    private static final long serialVersionUID = -5001927974502714777L;

    public NoDatasourceFoundException(String msg) {
        super(msg);
    }
}