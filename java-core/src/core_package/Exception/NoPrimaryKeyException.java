package core_package.Exception;

public class NoPrimaryKeyException extends Exception {
    public NoPrimaryKeyException() {
        super();
    }

    public NoPrimaryKeyException(String message) {
        super(message);
    }
}
