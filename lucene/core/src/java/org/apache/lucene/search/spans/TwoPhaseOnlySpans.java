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
package org.apache.lucene.search.spans;

import java.io.IOException;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.TwoPhaseIterator;

/**
 *
 */
public class TwoPhaseOnlySpans extends Spans {

  private final DocIdSetIterator backing;
  private final TwoPhaseIterator tpi;

  /**
   *
   * @param backing the backing DocIdSetIterator instance
   */
  public TwoPhaseOnlySpans(DocIdSetIterator backing) {
    this.backing = backing;
    this.tpi = new TwoPhaseDISIWrapper(backing);
  }

  @Override
  public int nextStartPosition() throws IOException {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public int startPosition() {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public int endPosition() {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public int width() {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public void collect(SpanCollector collector) throws IOException {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public float positionsCost() {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public int docID() {
    return backing.docID();
  }

  @Override
  public int nextDoc() throws IOException {
    return backing.nextDoc();
  }

  @Override
  public int advance(int target) throws IOException {
    return backing.advance(target);
  }

  @Override
  public long cost() {
    return backing.cost();
  }

  @Override
  public TwoPhaseIterator asTwoPhaseIterator() {
    return tpi;
  }

  private static class TwoPhaseDISIWrapper extends TwoPhaseIterator {

    public TwoPhaseDISIWrapper(DocIdSetIterator approximation) {
      super(approximation);
    }

    @Override
    public boolean matches() throws IOException {
      return true; // necessary filtering must be done by backing DISI
    }

    @Override
    public float matchCost() {
      return 1.0f;
    }

  }

}
