package org.elasticsearch.search.query.sortbydoc.utils;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.ESLoggerFactory;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * samuel
 * 22/10/15, 18:56
 */
public class XContentGetScoreMap {
    public static final Logger log = ESLoggerFactory.getLogger(XContentGetScoreMap.class);
    public static Map<String, Float> extractMap(Object part, String rootPath, String key, String val) {
        String[] pathElements = Strings.splitStringByCommaToArray(rootPath);

        // We expect only one
        for (int i = 0; i < pathElements.length; ++i) {
            if (!(part instanceof Map))
                return null;
            part = ((Map)part).get(pathElements[i]);
            if (i == pathElements.length - 1)
                break;
        }

        if (!(part instanceof List)) {
            return null;
        }

        Map<String, Float> values = new HashMap<>();
        for (Object o: (List)part) {
            if (!(o instanceof Map)) {
                return null;
            }
            Map item = (Map)o;
            Object itemKey = item.get(key);
            Object itemVal = item.get(val);

            if ((itemKey != null && itemKey instanceof String)) {
                if (itemVal == null) {
                    log.trace("Invalid value: null found");
                    values.put((String) itemKey, -Float.MAX_VALUE);
                    continue;
                }
                if (itemVal instanceof Number) {
                    values.put((String) itemKey, ((Number) itemVal).floatValue());
                } else if (itemVal instanceof Date) {
                    values.put((String) itemKey, (float) ((Date) itemVal).getTime());
                } else if (itemVal instanceof String) {
                    try {
                        Instant instant = Instant.parse(((String) itemVal).replace("+0000", "Z"));
                        values.put((String) itemKey, (float) instant.toEpochMilli());
                    } catch (DateTimeParseException dpe) {
                        log.trace("Invalid string value, cant parse date for item {}", itemVal);
                        values.put((String) itemKey, -Float.MAX_VALUE);
                    }
                } else {
                    log.trace("Invalid value for item {} class:{}", itemVal, itemVal.getClass().getName());
                    values.put((String) itemKey, -Float.MAX_VALUE);
                }
            }
        }

        return values;
    }
}
