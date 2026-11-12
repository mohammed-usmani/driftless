package dev.driftless.server.change;

import java.time.Instant;
import java.util.Map;

/**
 * One decoded source change.
 *
 * @param key canonical row key ({@link RowKeys}); null for a truncate
 * @param row the row after the change; null for deletes and truncates
 */
public record ChangeEvent(String pipeline, Op op, String key, Map<String, Object> row, Version version,
                          Instant committedAt, boolean snapshot, boolean synthetic) {
}
