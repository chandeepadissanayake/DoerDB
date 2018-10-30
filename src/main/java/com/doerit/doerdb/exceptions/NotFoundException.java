package com.doerit.doerdb.exceptions;

/**
 * NotFoundException is thrown when a required component cannot be found in the system.
 */
public class NotFoundException extends DoerDBException {

    public NotFoundException(int errorCode, String errorMessage) {
        this.setErrorCode(errorCode);
        this.setErrorMessage(errorMessage);
    }

}
