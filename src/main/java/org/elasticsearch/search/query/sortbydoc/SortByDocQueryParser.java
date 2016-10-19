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

import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.IdFieldMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryParser;
import org.elasticsearch.index.query.QueryParsingException;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.query.sortbydoc.utils.ScoresLookup;
import org.elasticsearch.search.query.sortbydoc.utils.XContentGetScoreMap;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;


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
public class SortByDocQueryParser implements QueryParser {
    public static final String NAME = "sort_by_doc";

    private Client client;

    @Inject
    public SortByDocQueryParser(Client client) {
        this.client = client;
    }

    @Override
    public String[] names() {
        return new String[]{NAME};
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        String currentFieldName = null;

        String lookupIndex = parseContext.index().name();
        String lookupType = null;
        String lookupId = null;
        String rootPath = null;
        String idField = null;
        String scoreField = null;
        String lookupRouting = null;
        SortOrder sortOrder = SortOrder.DESC;
        Query subQuery = null;

        XContentParser.Token token;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("query".equals(parser.currentName())) {
                    subQuery = parseContext.parseInnerQuery();
                    continue;
                }
            } else if (token.isValue()) {
                if (false) {
                } else if ("index".equals(currentFieldName)) {
                    lookupIndex = parser.text();
                } else if ("type".equals(currentFieldName)) {
                    lookupType = parser.text();
                } else if ("doc_id".equals(currentFieldName)) {
                    lookupId = parser.text();
                } else if ("root".equals(currentFieldName)) {
                    rootPath = parser.text();
                } else if ("id".equals(currentFieldName)) {
                    idField = parser.text();
                } else if ("score".equals(currentFieldName)) {
                    scoreField = parser.text();
                } else if ("routing".equals(currentFieldName)) {
                    lookupRouting = parser.textOrNull();
                } else if ("sort_order".equals(currentFieldName)) {
                    try {
                        sortOrder = SortOrder.valueOf(parser.text());
                    } catch (IllegalArgumentException e) {
                        throw new QueryParsingException(parseContext, "[sort_by_doc] sort_order should be one of " + Arrays.toString(SortOrder.values()));
                    }
                } else {
                    throw new QueryParsingException(parseContext, "[sort_by_doc] query does not support [" + currentFieldName + "] within lookup element");
                }
            }
        }
        if (lookupType == null) {
            throw new QueryParsingException(parseContext, "[sort_by_doc] query lookup element requires specifying the type");
        }
        if (lookupId == null) {
            throw new QueryParsingException(parseContext, "[sort_by_doc] query lookup element requires specifying the doc_id");
        }
        if (rootPath == null) {
            throw new QueryParsingException(parseContext, "[sort_by_doc] query lookup element requires specifying the path");
        }
        if (idField == null) {
            throw new QueryParsingException(parseContext, "[sort_by_doc] query lookup element requires specifying the id");
        }
        if (scoreField == null) {
            throw new QueryParsingException(parseContext, "[sort_by_doc] query lookup element requires specifying the score");
        }

        if (subQuery == null) {
            throw new QueryParsingException(parseContext, "[sort_by_doc] query requires a subquery");
        }

        MappedFieldType _idType = parseContext.mapperService().smartNameFieldType("_id");


        if (_idType == null || !(_idType.typeName().equals(IdFieldMapper.CONTENT_TYPE)))
            throw new QueryParsingException(parseContext, "[sort_by_doc] the _id field must be a defaultly indexed UID field");


        // external lookup of score values
        ScoresLookup lookup = new ScoresLookup(lookupIndex, lookupType, lookupId, lookupRouting, rootPath, idField, scoreField, parseContext);
        GetRequest request = new GetRequest(lookup.getIndex(), lookup.getType(), lookup.getId()).preference("_local").routing(lookup.getRouting());
        request.copyContextAndHeadersFrom(SearchContext.current());

        GetResponse getResponse = client.get(request).actionGet();

        // ids => scores
        Map<String, Float> scores = new HashMap<>();
        // Uid => scores
        Map<Term, Float> termsScores = new HashMap<>();

        if (getResponse.isExists()) {
            scores = XContentGetScoreMap.extractMap(getResponse.getSourceAsMap(), lookup.getObjectPath(), lookup.getKeyField(), lookup.getValField());
            if (scores == null) scores = new HashMap<>();
            final boolean isDesc = sortOrder.equals(SortOrder.DESC);
            scores.entrySet().forEach(score -> {
                BytesRef[] keyUids = Uid.createUidsForTypesAndId(parseContext.queryTypes(), score.getKey());
                for (BytesRef keyUid : keyUids) {
                    Term key = new Term(UidFieldMapper.NAME, keyUid);
                    termsScores.put(key, isDesc ? score.getValue() : -score.getValue());
                }
            });
        }
        if (scores.isEmpty()) {
            return subQuery;
        }

        // filter to only keep elements referenced in the lookup document
        Query filter = _idType.termsQuery(new ArrayList<>(scores.keySet()), parseContext);

        return new SortByDocQuery(_idType.names().indexName(), subQuery, filter, termsScores);
    }

}