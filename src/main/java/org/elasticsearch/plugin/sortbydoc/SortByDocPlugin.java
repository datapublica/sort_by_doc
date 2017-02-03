/* Copyright 2013 Endgame, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  
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
package org.elasticsearch.plugin.sortbydoc;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.query.sortbydoc.SortByDocQueryBuilder;
import org.elasticsearch.search.query.sortbydoc.SortByDocQueryParser;

import java.util.Collections;
import java.util.List;


public class SortByDocPlugin extends Plugin implements SearchPlugin {
    @Override
    public List<QuerySpec<?>> getQueries() {
        return Collections.singletonList(new QuerySpec<>(SortByDocQueryParser.NAME, SortByDocQueryBuilder::new, SortByDocQueryParser::fromXContent));
    }
}
