package core_package.Exception;

public class NoPrimaryKeyIdentifiedException extends Exception {
    public NoPrimaryKeyIdentifiedException() {
        super();
    }

    public NoPrimaryKeyIdentifiedException(String message) {
        super(message);
    }
}
