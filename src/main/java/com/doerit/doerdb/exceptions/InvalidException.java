package com.doerit.doerdb.exceptions;

public class InvalidException extends DoerDBException {

    public InvalidException(int errorCode, String errorMessage) {
        this.setErrorCode(errorCode);
        this.setErrorMessage(errorMessage);
    }

}
