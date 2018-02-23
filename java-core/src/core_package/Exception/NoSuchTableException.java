package core_package.Exception;

public class NoSuchTableException extends Exception{
    public NoSuchTableException() {
        super();
    }

    public NoSuchTableException(String message) {
        super(message);
    }
}
