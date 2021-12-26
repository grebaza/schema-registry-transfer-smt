/* Licensed under Apache-2.0 */
package cricket.jmoore.kafka.connect.transforms;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.function.Supplier;

public class Utils {
  private static final byte[] HEX_ARRAY = "0123456789ABCDEF".getBytes(StandardCharsets.US_ASCII);

  public static String bytesToHex(byte[] bytes) {
    byte[] hexChars = new byte[bytes.length * 2];
    for (int j = 0; j < bytes.length; j++) {
      int v = bytes[j] & 0xFF;
      hexChars[j * 2] = HEX_ARRAY[v >>> 4];
      hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
    }
    return new String(hexChars, StandardCharsets.UTF_8);
  }

  @FunctionalInterface
  public interface ThrowingSupplier<T> {
    T get() throws Exception;
  }

  public static <T> Supplier<Optional<T>> OptionalSupplier(ThrowingSupplier<T> supplier) {
    return () -> {
      try {
        return Optional.ofNullable(supplier.get());
      } catch (Exception e) {
        return Optional.empty();
      }
    };
  }

  public static <T> Supplier<T> RethrowingSupplier(ThrowingSupplier<T> supplier) {
    return () -> {
      try {
        return supplier.get();
      } catch (Exception e) {
        if (e instanceof RuntimeException) {
          throw (RuntimeException) e;
        } else {
          throw new RuntimeException(e);
        }
      }
    };
  }
}
