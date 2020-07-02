package home.dv.zkmon.util;

public class Pair<T, U> {

    private final T t;
    private final U u;

    private Pair(final T t, final U u) {
        this.t = t;
        this.u = u;
    }

    public static <T, U> Pair<T, U> of(final T t, final U u) {
        return new Pair<>(t, u);
    }

    public T getKey() {
        return t;
    }

    public U getValue() {
        return u;
    }

}
