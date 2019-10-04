/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.search.query.sortbydoc;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.Arrays;

import static org.elasticsearch.index.query.AbstractQueryBuilder.parseInnerQueryBuilder;


/**
 * <pre>
 * "sort_by_doc" : {
 *  "doc_id": "my_id"
 *  "type": "my_type"
 *  "index": "my_index"
 *  "root": "path_to_the_list_of_scores"
 *  "id": "field_for_ids"
 *  "score": "field_for_score"
 *  "query": {...}
 *  "sort_order: "ASC / DESC"
 * }
 * </pre>
 */
public class SortByDocQueryParser {
    public static final String NAME = "sort_by_doc";

    public static SortByDocQueryBuilder fromXContent(XContentParser parser) throws IOException {
        String currentFieldName = null;
        SortByDocQueryBuilder builder = new SortByDocQueryBuilder();

        XContentParser.Token token;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("query".equals(parser.currentName())) {
                    builder.query(parseInnerQueryBuilder(parser));
                    continue;
                }
            } else if (token.isValue() && currentFieldName != null) {
                if (false) {
                } else if ("index".equals(currentFieldName)) {
                    builder.lookupIndex(parser.text());
                } else if ("type".equals(currentFieldName)) {
                    // ignore
                } else if ("doc_id".equals(currentFieldName)) {
                    builder.lookupId(parser.text());
                } else if ("root".equals(currentFieldName)) {
                    builder.rootPath(parser.text());
                } else if ("id".equals(currentFieldName)) {
                    builder.idField(parser.text());
                } else if ("score".equals(currentFieldName)) {
                    builder.scoreField(parser.text());
                } else if ("routing".equals(currentFieldName)) {
                    builder.lookupRouting(parser.textOrNull());
                } else if ("max_score".equals(currentFieldName)) {
                    builder.maxScore(parser.floatValue());
                } else if ("min_score".equals(currentFieldName)) {
                    builder.minScore(parser.floatValue());
                } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    builder.queryName(parser.text());
                } else if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    builder.boost(parser.floatValue());
                } else if ("sort_order".equals(currentFieldName)) {
                    try {
                        builder.sortOrder(SortOrder.valueOf(parser.text()));
                    } catch (IllegalArgumentException e) {
                        throw new ParsingException(parser.getTokenLocation(), "[sort_by_doc] sort_order should be one of " + Arrays.toString(SortOrder.values()));
                    }
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[sort_by_doc] query does not support [" + currentFieldName + "] within lookup element");
                }
            }
        }

        builder.validate(str -> new ParsingException(parser.getTokenLocation(), str));
        return builder;
    }

}