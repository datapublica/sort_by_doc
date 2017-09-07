package org.elasticsearch.search.query.sortbydoc.scoring;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.elasticsearch.common.logging.ESLoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * samuel
 * 23/10/15, 15:18
 */
public class SortByDocScorer extends Scorer {
    public static final Logger log = ESLoggerFactory.getLogger(SortByDocScorer.class);
    private final DocIdSetIterator iterator;
    private Map<Integer, Float> scores;

    public SortByDocScorer(Map<Integer, Float> scores, DocIdSetIterator iterator, Weight weight) {
        super(weight);
        this.scores = scores;
        this.iterator = iterator;
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
    public int docID() {
        return iterator.docID();
    }


    @Override
    public int freq() throws IOException {
        return 1;
    }

    @Override
    public float score() throws IOException {
        Float value = scores.get(docID());
        return value == null ? 0 : value;
    }
}
