package meridian.stream;

import java.util.Objects;

/**
 * 4-tuple helper for Flink stream operations.
 */
public class Tuple4<A, B, C, D> {
    public A f0;
    public B f1;
    public C f2;
    public D f3;

    public Tuple4() {}

    public Tuple4(A f0, B f1, C f2, D f3) {
        this.f0 = f0;
        this.f1 = f1;
        this.f2 = f2;
        this.f3 = f3;
    }

    public static <A, B, C, D> Tuple4<A, B, C, D> of(A a, B b, C c, D d) {
        return new Tuple4<>(a, b, c, d);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Tuple4)) return false;
        Tuple4<?, ?, ?, ?> tuple4 = (Tuple4<?, ?, ?, ?>) o;
        return Objects.equals(f0, tuple4.f0) && Objects.equals(f1, tuple4.f1)
                && Objects.equals(f2, tuple4.f2) && Objects.equals(f3, tuple4.f3);
    }

    @Override
    public int hashCode() {
        return Objects.hash(f0, f1, f2, f3);
    }
}
