/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.search;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.CommonParams;
import org.junit.BeforeClass;

/**
 * Test for Fuzzy Query was taken from org.apache.lucene.search.TestFuzzyQuery
 * SOLR-12761 fuzzy query
 */
public class TestFuzzyQueryParser extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty("enable.update.log", "false"); // schema12 doesn't support _version_
    initCore("solrconfig.xml", "schema12.xml");
    index();
  }

  public static void index() throws Exception {
    assertU(adoc("id", "101", "text", "aaaaa"));
    assertU(adoc("id", "102", "text", "aaaab"));
    assertU(adoc("id", "103", "text", "aaabb"));
    assertU(adoc("id", "104", "text", "aabbb"));
    assertU(adoc("id", "105", "text", "abbbb"));
    assertU(adoc("id", "106", "text", "bbbbb"));
    assertU(adoc("id", "107", "text", "ddddd"));

    assertU(adoc("id", "201", "text", "a123456"));
    assertU(adoc("id", "202", "text", "b123456"));
    assertU(adoc("id", "203", "text", "b123456"));
    assertU(adoc("id", "204", "text", "b123456"));
    assertU(adoc("id", "205", "text", "c123456"));
    assertU(adoc("id", "206", "text", "f123456"));
    assertU(adoc("id", "207", "text", "a123456"));
    assertU(adoc("id", "208", "text", "c123456"));
    assertU(adoc("id", "209", "text", "d123456"));
    assertU(adoc("id", "210", "text", "e123456"));

    assertU(adoc("id", "301", "text", "foobar"));
    assertU(adoc("id", "302", "text", "test"));
    assertU(adoc("id", "303", "text", "working"));

    assertU(adoc("id", "401", "text", "michael smith"));
    assertU(adoc("id", "402", "text", "michael lucero"));
    assertU(adoc("id", "404", "text", "doug cuttin"));
    assertU(adoc("id", "405", "text", "michael wardle"));
    assertU(adoc("id", "406", "text", "micheal vegas"));
    assertU(adoc("id", "407", "text", "michael lydon"));

    assertU(commit());
  }

  public void testFuzzyQueryParser() throws Exception {
    assertJQ(req("qf", "text", "q", "aaaaa~2"
        , CommonParams.FUZZY_PREFIX_LENGTH, "3")
        , "/response/numFound==3"
    );
    assertJQ(req("qf", "text", "q", "aaaaa~2"
        , CommonParams.FUZZY_PREFIX_LENGTH, "4")
        , "/response/numFound==2"
    );
    assertJQ(req("qf", "text", "q", "aaaaa~2"
        , CommonParams.FUZZY_PREFIX_LENGTH, "5")
        , "/response/numFound==1"
    );

    assertJQ(req("qf", "text", "q", "bbbbb~2"
        , CommonParams.FUZZY_PREFIX_LENGTH, "0")
        , "/response/numFound==3"
    );
    assertJQ(req("qf", "text", "q", "bbbbb~2"
        , CommonParams.FUZZY_PREFIX_LENGTH, "0"
        , CommonParams.FUZZY_MAX_EXPANSIONS, "2"
        , CommonParams.FUZZY_TRANSPOSITIONS, "false")
        , "/response/numFound==2"
    );

    assertJQ(req("qf", "text", "q", "x123456~1"
        , CommonParams.FUZZY_PREFIX_LENGTH, "0"
        , CommonParams.FUZZY_MAX_EXPANSIONS, "2"
        , CommonParams.FUZZY_TRANSPOSITIONS, "false")
        , "/response/numFound==10"
    );

    assertJQ(req("qf", "text", "q", "fouba~2")
        , "/response/numFound==1"
    );
    assertJQ(req("qf", "text", "q", "fouba~2")
        , "/response/docs/[0]/id==\"301\""
    );
    assertJQ(req("qf", "text", "q", "foubara~2")
        , "/response/numFound==1"
    );

    assertJQ(req("qf", "text", "q", "michael~2 OR cutting~2"
        , CommonParams.FUZZY_PREFIX_LENGTH, "2")
        , "/response/numFound==6"
    );
  }

}
