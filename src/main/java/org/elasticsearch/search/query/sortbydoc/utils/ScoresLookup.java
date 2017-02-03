package org.elasticsearch.search.query.sortbydoc.utils;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.query.QueryParseContext;

/**
 * samuel
 * 22/10/15, 18:49
 */
public class ScoresLookup {
    private final String index;
    private final String type;
    private final String id;
    private final String routing;
    private final String objectPath;
    private final String keyField;
    private final String valField;

    public ScoresLookup(String index, String type, String id, String routing, String objectPath, String keyField, String valField) {
        this.index = index;
        this.type = type;
        this.id = id;
        this.routing = routing;
        this.objectPath = objectPath;
        this.keyField = keyField;
        this.valField = valField;
    }

    public String getIndex() {
        return index;
    }

    public String getType() {
        return type;
    }

    public String getId() {
        return id;
    }

    public String getRouting() {
        return this.routing;
    }

    public String getKeyField() {
        return keyField;
    }

    public String getValField() {
        return valField;
    }

    public String getObjectPath() { return objectPath; }

    public String toString() {
        return index + "/" + type + "/" + id + "/" + keyField + "/" + valField;
    }
}
