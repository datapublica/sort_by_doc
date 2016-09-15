package org.elasticsearch.search.query.sortbydoc.scoring;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * samuel
 * 22/10/15, 14:55
 */
public class SortByDocWeight extends Weight {
    private final String fieldName;
    private Map<Term, Float> scores;
    private Query query;

    public SortByDocWeight(Query query, String fieldName, Map<Term, Float> scores) {
        super(query);
        this.scores = scores;
        this.query = query;
        this.fieldName = fieldName;
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
        Float score = getScores(context).get(doc);
        return Explanation.match(score, "sort_by_doc");
    }

    @Override
    public void extractTerms(Set<Term> set) {
    }

    @Override
    public float getValueForNormalization() throws IOException {
        return 1;
    }

    @Override
    public void normalize(float norm, float topLevelBoost) {
        // Do nothing since we are assigning a custom score to each doc
    }


    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {
        // only score if parent document ie: context.docBaseInParent == 0
        if (context.docBaseInParent == 0)
            return new SortByDocScorer(getScores(context), DocIdSetIterator.all(context.reader().maxDoc()), this);
        else
            return null;
    }

    private Map<Integer, Float> getScores(LeafReaderContext context) throws IOException {
        Map<Integer, Float> scores = new HashMap<>();
        TermsEnum termsIterator = context.reader().fields().terms(UidFieldMapper.NAME).iterator();

        for (Map.Entry<Term, Float> score : this.scores.entrySet()) {
            if (!termsIterator.seekExact(score.getKey().bytes())) {
                // Term not found
                continue;
            }

            PostingsEnum postings = termsIterator.postings(null);
            if (postings.nextDoc() == DocIdSetIterator.NO_MORE_DOCS)
                continue;
            scores.put(postings.docID(), score.getValue());
        }

        return scores;
    }

}
