package org.elasticsearch.search.query.sortbydoc;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.elasticsearch.search.query.sortbydoc.scoring.SortByDocWeight;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * samuel
 * 22/10/15, 14:11
 */
public class SortByDocQuery extends Query {
    private final String fieldName;
    private Query subQuery;
    private Map<Term, Float> scores;

    SortByDocQuery(String fieldName, Query subQuery, Query filter, Map<Term, Float> scores) {
        this.fieldName = fieldName;
        this.subQuery = new BooleanQuery.Builder()
                .add(subQuery, BooleanClause.Occur.MUST)
                .add(filter, BooleanClause.Occur.FILTER).build();
        this.scores = scores;
    }

    private SortByDocQuery(Query subQuery, String fieldName, Map<Term, Float> scores) {
        this.subQuery = subQuery;
        this.fieldName = fieldName;
        this.scores = scores;
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        Query newSubQuery = subQuery.rewrite(reader);
        if (newSubQuery == subQuery)
            return this;
        SortByDocQuery newQuery = new SortByDocQuery(subQuery, fieldName, scores);
        newQuery.subQuery = newSubQuery;
        return newQuery;
    }

    @Override
    public String toString(String s) {
        return "sort-by-doc("+fieldName+")";
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
        return new SortByDocWeight(this, fieldName, scores, subQuery.createWeight(searcher, needsScores));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SortByDocQuery that = (SortByDocQuery) o;
        return Objects.equals(fieldName, that.fieldName) &&
                Objects.equals(subQuery, that.subQuery) &&
                Objects.equals(scores, that.scores);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldName, subQuery, scores);
    }
}
