package com.doerit.doerdb.exceptions;

public class SynchronizeException extends DoerDBException {

    public SynchronizeException(int errorCode, String errorMessage) {
        this.setErrorCode(errorCode);
        this.setErrorMessage(errorMessage);
    }

}
