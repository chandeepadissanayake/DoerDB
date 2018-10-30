package com.doerit.doerdb.exceptions;

/**
 * InterruptedException is thrown whenever any process is interrupted by an unexpected interception.
 */
public class InterruptedException extends DoerDBException {

    public InterruptedException(int errorCode, String errorMessage) {
        this.setErrorCode(errorCode);
        this.setErrorMessage(errorMessage);
    }

}
