package org.elasticsearch.search.query.sortbydoc.scoring;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.*;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.lucene.uid.Versions;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * samuel
 * 22/10/15, 14:55
 */
public class SortByDocWeight extends Weight {
    public static final Logger log = ESLoggerFactory.getLogger(SortByDocWeight.class);
    private Weight weight;
    private Map<Term, Float> scores;

    public SortByDocWeight(Query query, Map<Term, Float> scores, Weight weight) {
        super(query);
        this.scores = scores;
        this.weight = weight;
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
        Scorer scorer = weight.scorer(context);
        if (scorer == null) {
            return null;
        }
        return new SortByDocScorer(getScores(context), scorer.iterator(), this);
    }

    private Map<Integer, Float> getScores(LeafReaderContext context) throws IOException {
        log.trace("[getScores] Content of the score table (size: {})", this.scores.size());
        Map<Integer, Float> scores = new HashMap<>();
        LeafReader reader = context.reader();
        for (Map.Entry<Term, Float> score : this.scores.entrySet()) {
            Versions.DocIdAndVersion docIdAndVersion = Versions.loadDocIdAndVersion(reader, score.getKey());
            if (docIdAndVersion == null) {
                log.trace("[getScores] Could not find postings {}", score.getKey().text());
                continue;
            }
            scores.put(docIdAndVersion.docId, score.getValue());
        }
        log.trace("[getScores] Content of the internal score table (size: {}) {}",scores.size(), scores);

        return scores;
    }

}
