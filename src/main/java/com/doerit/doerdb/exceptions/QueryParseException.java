package com.doerit.doerdb.exceptions;

public class QueryParseException extends DoerDBException {

    public QueryParseException(int errorCode, String errorMessage) {
        this.setErrorCode(errorCode);
        this.setErrorMessage(errorMessage);
    }

}
