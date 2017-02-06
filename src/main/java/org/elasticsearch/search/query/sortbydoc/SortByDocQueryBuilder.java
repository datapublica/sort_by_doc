package org.elasticsearch.search.query.sortbydoc;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.UidFieldMapper;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.query.sortbydoc.utils.ScoresLookup;
import org.elasticsearch.search.query.sortbydoc.utils.XContentGetScoreMap;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * samuel
 * 19/11/15, 15:57
 */
public class SortByDocQueryBuilder extends AbstractQueryBuilder {
    private String lookupIndex;
    private String lookupType;
    private String lookupId;
    private String lookupRouting;

    private String rootPath;
    private String idField;
    private String scoreField;
    private QueryBuilder subQuery;
    private SortOrder sortOrder;

    public SortByDocQueryBuilder() {
    }

    public SortByDocQueryBuilder(StreamInput in) throws IOException {
        this.lookupIndex = in.readString();
        this.lookupType = in.readString();
        this.lookupId = in.readString();
        this.lookupRouting = in.readOptionalString();
        this.rootPath = in.readString();
        this.idField = in.readString();
        this.scoreField = in.readString();
        this.sortOrder = SortOrder.values()[in.readInt()];
        subQuery = in.readNamedWriteable(QueryBuilder.class);
    }

    public SortByDocQueryBuilder(String lookupIndex, String lookupType, String lookupId, String lookupRouting, String rootPath, String idField, String scoreField, QueryBuilder subQuery, SortOrder sortOrder) {
        this.lookupIndex = lookupIndex;
        this.lookupType = lookupType;
        this.lookupId = lookupId;
        this.lookupRouting = lookupRouting;
        this.rootPath = rootPath;
        this.idField = idField;
        this.scoreField = scoreField;
        this.subQuery = subQuery;
        this.sortOrder = sortOrder;
    }

    @Override
    public String getWriteableName() {
        return SortByDocQueryParser.NAME;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(lookupIndex);
        out.writeString(lookupType);
        out.writeString(lookupId);
        out.writeOptionalString(lookupRouting);
        out.writeString(rootPath);
        out.writeString(idField);
        out.writeString(scoreField);
        out.writeInt(sortOrder.ordinal());
        subQuery.writeTo(out);
    }

    /**
     * Sets the query to filter & sort
     */
    public SortByDocQueryBuilder query(QueryBuilder subQuery) {
        this.subQuery = subQuery;
        return this;
    }

    /**
     * Sets the routing for the doc to lookup
     */
    public SortByDocQueryBuilder lookupRouting(String lookupRouting) {
        this.lookupRouting = lookupRouting;
        return this;
    }

    /**
     * Sets the index name to lookup the terms from.
     */
    public SortByDocQueryBuilder lookupIndex(String lookupIndex) {
        this.lookupIndex = lookupIndex;
        return this;
    }

    /**
     * Sets the index type to lookup the terms from.
     */
    public SortByDocQueryBuilder lookupType(String lookupType) {
        this.lookupType = lookupType;
        return this;
    }

    /**
     * Sets the doc id to lookup the terms from.
     */
    public SortByDocQueryBuilder lookupId(String lookupId) {
        this.lookupId = lookupId;
        return this;
    }

    /**
     * Sets the path within the document to lookup the items from.
     */
    public SortByDocQueryBuilder rootPath(String rootPath) {
        this.rootPath = rootPath;
        return this;
    }

    /**
     * Sets the field name to retrieve ids in objects found at rootPath
     */
    public SortByDocQueryBuilder idField(String idField) {
        this.idField = idField;
        return this;
    }

    /**
     * Sets the field name to retrieve scores in objects found at rootPath
     */
    public SortByDocQueryBuilder scoreField(String scoreField) {
        this.scoreField = scoreField;
        return this;
    }

    /**
     * Sets the field name to retrieve scores in objects found at rootPath
     */
    public SortByDocQueryBuilder sortOrder(SortOrder sortOrder) {
        this.sortOrder = sortOrder;
        return this;
    }

    public void validate(Function<String, ElasticsearchException> exceptionProvider) throws IOException {
        if (lookupType == null) {
            throw exceptionProvider.apply("[sort_by_doc] query lookup element requires specifying the type");
        }
        if (lookupId == null) {
            throw exceptionProvider.apply("[sort_by_doc] query lookup element requires specifying the doc_id");
        }
        if (lookupIndex == null) {
            throw exceptionProvider.apply("[sort_by_doc] query lookup element requires specifying the index");
        }
        if (rootPath == null) {
            throw exceptionProvider.apply("[sort_by_doc] query lookup element requires specifying the path");
        }
        if (idField == null) {
            throw exceptionProvider.apply("[sort_by_doc] query lookup element requires specifying the id");
        }
        if (scoreField == null) {
            throw exceptionProvider.apply("[sort_by_doc] query lookup element requires specifying the score");
        }
        if (subQuery == null) {
            throw exceptionProvider.apply("[sort_by_doc] query requires a subquery");
        }
        if (sortOrder == null) {
            throw exceptionProvider.apply("[sort_by_doc] query lookup element requires specifying the score");
        }
    }

    @Override
    protected boolean doEquals(AbstractQueryBuilder o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SortByDocQueryBuilder that = (SortByDocQueryBuilder) o;
        return Objects.equals(lookupIndex, that.lookupIndex) &&
                Objects.equals(lookupType, that.lookupType) &&
                Objects.equals(lookupId, that.lookupId) &&
                Objects.equals(lookupRouting, that.lookupRouting) &&
                Objects.equals(rootPath, that.rootPath) &&
                Objects.equals(idField, that.idField) &&
                Objects.equals(scoreField, that.scoreField) &&
                Objects.equals(subQuery, that.subQuery) &&
                sortOrder == that.sortOrder;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(lookupIndex, lookupType, lookupId, lookupRouting, rootPath, idField, scoreField, subQuery, sortOrder);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(SortByDocQueryParser.NAME);

        if (lookupIndex != null) {
            builder.field("index", lookupIndex);
        }
        builder.field("type", lookupType);
        builder.field("doc_id", lookupId);
        if (lookupRouting != null) {
            builder.field("routing", lookupRouting);
        }

        if (subQuery != null) {
            builder.field("query");
            subQuery.toXContent(builder, params);
        }

        builder.field("root", rootPath);
        builder.field("id", idField);
        builder.field("score", scoreField);
        builder.field("sort_order", sortOrder.name());
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        MappedFieldType _idType = context.getMapperService().fullName("_id");


        if (_idType == null || !(_idType.typeName().equals(IdFieldMapper.CONTENT_TYPE)))
            throw new IllegalArgumentException("[sort_by_doc] the _id field must be a defaultly indexed UID field");


        // external lookup of score values
        ScoresLookup lookup = new ScoresLookup(lookupIndex, lookupType, lookupId, lookupRouting, rootPath, idField, scoreField);
        GetRequest request = new GetRequest(lookup.getIndex(), lookup.getType(), lookup.getId()).preference("_local").routing(lookup.getRouting());
//        request.copyContextAndHeadersFrom(SearchContext.current());

        GetResponse getResponse = context.getClient().get(request).actionGet();

        // ids => scores
        Map<String, Float> scores = new HashMap<>();
        // Uid => scores
        Map<Term, Float> termsScores = new HashMap<>();

        if (getResponse.isExists()) {
            scores = XContentGetScoreMap.extractMap(getResponse.getSourceAsMap(), lookup.getObjectPath(), lookup.getKeyField(), lookup.getValField());
            if (scores == null) scores = new HashMap<>();
            final boolean isDesc = sortOrder.equals(SortOrder.DESC);
            scores.entrySet().forEach(score -> {
                BytesRef[] keyUids = Uid.createUidsForTypesAndId(context.queryTypes(), score.getKey());
                for (BytesRef keyUid : keyUids) {
                    Term key = new Term(UidFieldMapper.NAME, keyUid);
                    termsScores.put(key, isDesc ? score.getValue() : -score.getValue());
                }
            });
        }
        if (scores.isEmpty()) {
            return subQuery.toQuery(context);
        }

        // filter to only keep elements referenced in the lookup document
        Query filter = _idType.termsQuery(new ArrayList<>(scores.keySet()), context);

        return new SortByDocQuery(context.index().getName(), subQuery.toQuery(context), filter, termsScores);
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryShardContext) throws IOException {
        QueryBuilder newSubQuery = subQuery.rewrite(queryShardContext);
        if (newSubQuery == subQuery)
            return this;
        return new SortByDocQueryBuilder(lookupIndex, lookupType, lookupId, lookupRouting, rootPath, idField, scoreField, newSubQuery, sortOrder);
    }
}
