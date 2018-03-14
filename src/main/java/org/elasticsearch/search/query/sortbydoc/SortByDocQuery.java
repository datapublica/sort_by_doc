package org.elasticsearch.search.query.sortbydoc;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.search.query.sortbydoc.scoring.SortByDocWeight;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * samuel
 * 22/10/15, 14:11
 */
public class SortByDocQuery extends Query {
    private Query subQuery;
    private Map<BytesRef, Float> scores;

    SortByDocQuery(Query subQuery, Query filter, Map<BytesRef, Float> scores) {
        this.subQuery = new BooleanQuery.Builder()
                .add(subQuery, BooleanClause.Occur.MUST)
                .add(filter, BooleanClause.Occur.FILTER).build();
        this.scores = scores;
    }

    private SortByDocQuery(Query subQuery, Map<BytesRef, Float> scores) {
        this.subQuery = subQuery;
        this.scores = scores;
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        Query newSubQuery = subQuery.rewrite(reader);
        if (newSubQuery == subQuery)
            return this;
        SortByDocQuery newQuery = new SortByDocQuery(subQuery, scores);
        newQuery.subQuery = newSubQuery;
        return newQuery;
    }

    @Override
    public String toString(String s) {
        return "sort-by-doc";
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores, float boost) throws IOException {
        return new SortByDocWeight(this, scores, searcher, subQuery.createWeight(searcher, needsScores, boost));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SortByDocQuery that = (SortByDocQuery) o;
        return Objects.equals(subQuery, that.subQuery) &&
                Objects.equals(scores, that.scores);
    }

    @Override
    public int hashCode() {
        return Objects.hash(subQuery, scores);
    }
}
