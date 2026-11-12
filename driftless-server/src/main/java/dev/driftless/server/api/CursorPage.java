package dev.driftless.server.api;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.function.Function;
import java.util.function.ToLongFunction;
import org.springframework.http.HttpStatus;

/** One page of a newest-first list keyed by a bigserial id; {@code nextCursor} is null on the last page. */
public record CursorPage<T>(List<T> data, String nextCursor) {

    public static final int DEFAULT_LIMIT = 50;
    public static final int MAX_LIMIT = 200;

    public static int clampLimit(Integer limit) {
        if (limit == null) {
            return DEFAULT_LIMIT;
        }
        if (limit < 1 || limit > MAX_LIMIT) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "limit must be between 1 and " + MAX_LIMIT);
        }
        return limit;
    }

    /** The id to page below; Long.MAX_VALUE for the first page. */
    public static long before(String cursor) {
        if (cursor == null || cursor.isBlank()) {
            return Long.MAX_VALUE;
        }
        try {
            return Long.parseLong(new String(Base64.getUrlDecoder().decode(cursor), StandardCharsets.UTF_8));
        } catch (IllegalArgumentException e) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "invalid cursor");
        }
    }

    /** Builds a page from rows fetched with {@code limit + 1}; the extra row only says another page exists. */
    public static <R, T> CursorPage<T> of(List<R> rows, int limit, ToLongFunction<R> id, Function<R, T> mapper) {
        boolean more = rows.size() > limit;
        List<R> page = more ? rows.subList(0, limit) : rows;
        String next = more ? Base64.getUrlEncoder().withoutPadding()
                .encodeToString(Long.toString(id.applyAsLong(page.getLast())).getBytes(StandardCharsets.UTF_8)) : null;
        return new CursorPage<>(page.stream().map(mapper).toList(), next);
    }
}
