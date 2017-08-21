package com.bohaisoft.dal.client.exception;

import org.springframework.dao.UncategorizedDataAccessException;

public class UncategorizedClientException extends UncategorizedDataAccessException {
    private static final long serialVersionUID = -5001927974502714777L;

    public UncategorizedClientException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
