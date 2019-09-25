package org.elasticsearch.search.query.sortbydoc.scoring;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * samuel
 * 23/10/15, 15:18
 */
public class SortByDocScorer extends Scorer {
    private static final Logger log = LogManager.getLogger(SortByDocScorer.class);
    private final DocIdSetIterator iterator;
    private Map<Integer, Float> scores;
    private float max;

    SortByDocScorer(Map<Integer, Float> scores, DocIdSetIterator iterator, Weight weight) {
        super(weight);
        this.scores = scores;
        this.iterator = iterator;
        if (!scores.isEmpty()) {
            max = Collections.max(scores.values());
        }
    }

    @Override
    public DocIdSetIterator iterator() {

        return new DocIdSetIterator() {
            @Override
            public int docID() {
                return iterator.docID();
            }

            @Override
            public int nextDoc() throws IOException {
                // retrieve the next document indluded in the scores
                int docId;
                while ((docId = iterator.nextDoc()) != NO_MORE_DOCS) {
                    if (scores.containsKey(docId)) {
                        return docId;
                    } else {
                        log.trace("[nextdoc] Skipping document {}", docId);
                    }
                }
                return NO_MORE_DOCS;
            }

            @Override
            public int advance(int target) throws IOException {
                int docId = iterator.advance(target);
                if (docId != NO_MORE_DOCS) {
                    // We advanced, but if the document was not in our score set (for whatever reason)
                    // then we go to the next valid document by calling nextDoc
                    if (scores.containsKey(docId))
                        return docId;
                    log.trace("[advance] Skipping document {}", docId);
                    return nextDoc();
                }
                return NO_MORE_DOCS;
            }

            @Override
            public long cost() {
                return iterator.cost();
            }
        };
    }

    @Override
    public float getMaxScore(int i) {
        return max;
    }

    @Override
    public int docID() {
        return iterator.docID();
    }

    @Override
    public float score() {
        Float value = scores.get(docID());
        return value == null ? 0 : value;
    }
}
