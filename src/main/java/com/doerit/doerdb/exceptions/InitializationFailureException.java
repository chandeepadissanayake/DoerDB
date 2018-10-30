package com.doerit.doerdb.exceptions;

/**
 * InitializationFailureException is thrown when any class fails to initialize.
 */
public class InitializationFailureException extends DoerDBException {

    public InitializationFailureException(int errorCode, String errorMessage) {
        this.setErrorCode(errorCode);
        this.setErrorMessage(errorMessage);
    }

}
