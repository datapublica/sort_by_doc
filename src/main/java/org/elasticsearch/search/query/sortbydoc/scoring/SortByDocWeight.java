package org.elasticsearch.search.query.sortbydoc.scoring;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.uid.VersionsAndSeqNoResolver;
import org.elasticsearch.index.mapper.IdFieldMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * samuel
 * 22/10/15, 14:55
 */
public class SortByDocWeight extends Weight {
    private static final Logger log = LogManager.getLogger(SortByDocWeight.class);
    private Weight weight;
    private Map<BytesRef, Float> scores;

    public SortByDocWeight(Query query, Map<BytesRef, Float> scores, Weight weight) {
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
    public Scorer scorer(LeafReaderContext context) throws IOException {
        Scorer scorer = weight.scorer(context);
        if (scorer == null) {
            return null;
        }
        return new SortByDocScorer(getScores(context), scorer.iterator(), this);
    }

    private Map<Integer, Float> getScores(LeafReaderContext context) throws IOException {
        log.trace("[getScores] Content of the score table (size: {}) {}", this.scores.size(), context.reader().terms(IdFieldMapper.NAME).getMin());
        Map<Integer, Float> scores = new HashMap<>();
        LeafReader reader = context.reader();
        for (Map.Entry<BytesRef, Float> score : this.scores.entrySet()) {
            VersionsAndSeqNoResolver.DocIdAndVersion docIdAndVersion = VersionsAndSeqNoResolver.loadDocIdAndVersion(reader, new Term(IdFieldMapper.NAME, score.getKey()), false);
            if (docIdAndVersion == null) {
                log.trace("[getScores] Could not find postings {}", score.getKey());
                continue;
            }
            scores.put(docIdAndVersion.docId, score.getValue());
        }

        log.trace("[getScores] Content of the internal score table (size: {}) {}",scores.size(), scores);

        return scores;
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
        return false;
    }
}
