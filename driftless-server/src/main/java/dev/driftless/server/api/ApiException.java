package dev.driftless.server.api;

import org.springframework.http.HttpStatus;

/** An error the API turns into a problem response with this status. */
public class ApiException extends RuntimeException {

    private final HttpStatus status;

    public ApiException(HttpStatus status, String message) {
        super(message);
        this.status = status;
    }

    public HttpStatus status() {
        return status;
    }
}
