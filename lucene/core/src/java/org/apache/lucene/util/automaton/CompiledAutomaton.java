package org.apache.lucene.util.automaton;

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
  
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.PrefixTermsEnum;
import org.apache.lucene.index.SingleTermsEnum;
import org.apache.lucene.util.BytesRef;

/**
 * Immutable class holding compiled details for a given
 * Automaton.  The Automaton is deterministic, must not have
 * dead states but is not necessarily minimal.
 *
 * @lucene.experimental
 */
public class CompiledAutomaton {
  /**
   * Automata are compiled into different internal forms for the
   * most efficient execution depending upon the language they accept.
   */
  public enum AUTOMATON_TYPE {
    /** Automaton that accepts no strings. */
    NONE, 
    /** Automaton that accepts all possible strings. */
    ALL, 
    /** Automaton that accepts only a single fixed string. */
    SINGLE, 
    /** Automaton that matches all Strings with a constant prefix. */
    PREFIX, 
    /** Catch-all for any other automata. */
    NORMAL
  };
  public final AUTOMATON_TYPE type;

  /** 
   * For {@link AUTOMATON_TYPE#PREFIX}, this is the prefix term; 
   * for {@link AUTOMATON_TYPE#SINGLE} this is the singleton term.
   */
  public final BytesRef term;

  /** 
   * Matcher for quickly determining if a byte[] is accepted.
   * only valid for {@link AUTOMATON_TYPE#NORMAL}.
   */
  public final ByteRunAutomaton runAutomaton;

  /**
   * Two dimensional array of transitions, indexed by state
   * number for traversal. The state numbering is consistent with
   * {@link #runAutomaton}. 
   * Only valid for {@link AUTOMATON_TYPE#NORMAL}.
   */
  public final LightAutomaton lightAutomaton;
  /**
   * Shared common suffix accepted by the automaton. Only valid
   * for {@link AUTOMATON_TYPE#NORMAL}, and only when the
   * automaton accepts an infinite language.
   */
  public final BytesRef commonSuffixRef;
  /**
   * Indicates if the automaton accepts a finite set of strings.
   * Null if this was not computed.
   * Only valid for {@link AUTOMATON_TYPE#NORMAL}.
   */
  public final Boolean finite;

  public CompiledAutomaton(LightAutomaton automaton) {
    this(automaton, null, true);
  }

  public CompiledAutomaton(LightAutomaton automaton, Boolean finite, boolean simplify) {

    if (simplify) {

      // Test whether the automaton is a "simple" form and
      // if so, don't create a runAutomaton.  Note that on a
      // large automaton these tests could be costly:

      if (BasicOperations.isEmpty(automaton)) {
        // matches nothing
        type = AUTOMATON_TYPE.NONE;
        term = null;
        commonSuffixRef = null;
        runAutomaton = null;
        lightAutomaton = null;
        this.finite = null;
        return;
      // NOTE: only approximate, because automaton may not be minimal:
      } else if (BasicOperations.isTotal(automaton)) {
        // matches all possible strings
        type = AUTOMATON_TYPE.ALL;
        term = null;
        commonSuffixRef = null;
        runAutomaton = null;
        lightAutomaton = null;
        this.finite = null;
        return;
      } else {

        automaton = BasicOperations.determinize(automaton);

        final String commonPrefix = SpecialOperations.getCommonPrefix(automaton);
        final String singleton;

        if (commonPrefix.length() > 0 && BasicOperations.sameLanguage(automaton, BasicAutomata.makeStringLight(commonPrefix))) {
          singleton = commonPrefix;
        } else {
          singleton = null;
        }

        if (singleton != null) {
          // matches a fixed string
          type = AUTOMATON_TYPE.SINGLE;
          term = new BytesRef(singleton);
          commonSuffixRef = null;
          runAutomaton = null;
          lightAutomaton = null;
          this.finite = null;
          return;
        } else if (commonPrefix.length() > 0) {
          LightAutomaton other = BasicOperations.concatenateLight(BasicAutomata.makeStringLight(commonPrefix), BasicAutomata.makeAnyStringLight());
          other = BasicOperations.determinize(other);
          assert BasicOperations.hasDeadStates(other) == false;
          if (BasicOperations.sameLanguage(automaton, other)) {
            // matches a constant prefix
            type = AUTOMATON_TYPE.PREFIX;
            term = new BytesRef(commonPrefix);
            commonSuffixRef = null;
            runAutomaton = null;
            lightAutomaton = null;
            this.finite = null;
            return;
          }
        }
      }
    }

    type = AUTOMATON_TYPE.NORMAL;
    term = null;

    if (finite == null) {
      this.finite = SpecialOperations.isFinite(automaton);
    } else {
      this.finite = finite;
    }

    LightAutomaton utf8 = new UTF32ToUTF8Light().convert(automaton);
    if (this.finite) {
      commonSuffixRef = null;
    } else {
      commonSuffixRef = SpecialOperations.getCommonSuffixBytesRef(utf8);
    }
    runAutomaton = new ByteRunAutomaton(utf8, true);

    lightAutomaton = runAutomaton.automaton;
  }

  private Transition transition = new Transition();
  
  //private static final boolean DEBUG = BlockTreeTermsWriter.DEBUG;

  private BytesRef addTail(int state, BytesRef term, int idx, int leadLabel) {
    //System.out.println("addTail state=" + state + " term=" + term.utf8ToString() + " idx=" + idx + " leadLabel=" + (char) leadLabel);
    //System.out.println(lightAutomaton.toDot());
    // Find biggest transition that's < label
    // TODO: use binary search here
    int maxIndex = -1;
    int numTransitions = lightAutomaton.initTransition(state, transition);
    for(int i=0;i<numTransitions;i++) {
      lightAutomaton.getNextTransition(transition);
      if (transition.min < leadLabel) {
        maxIndex = i;
      } else {
        // Transitions are alway sorted
        break;
      }
    }

    //System.out.println("  maxIndex=" + maxIndex);

    assert maxIndex != -1;
    lightAutomaton.getTransition(state, maxIndex, transition);

    // Append floorLabel
    final int floorLabel;
    if (transition.max > leadLabel-1) {
      floorLabel = leadLabel-1;
    } else {
      floorLabel = transition.max;
    }
    //System.out.println("  floorLabel=" + (char) floorLabel);
    if (idx >= term.bytes.length) {
      term.grow(1+idx);
    }
    //if (DEBUG) System.out.println("  add floorLabel=" + (char) floorLabel + " idx=" + idx);
    term.bytes[idx] = (byte) floorLabel;

    state = transition.dest;
    //System.out.println("  dest: " + state);
    idx++;

    // Push down to last accept state
    while (true) {
      numTransitions = lightAutomaton.getNumTransitions(state);
      if (numTransitions == 0) {
        //System.out.println("state=" + state + " 0 trans");
        assert runAutomaton.isAccept(state);
        term.length = idx;
        //if (DEBUG) System.out.println("  return " + term.utf8ToString());
        return term;
      } else {
        // We are pushing "top" -- so get last label of
        // last transition:
        //System.out.println("get state=" + state + " numTrans=" + numTransitions);
        lightAutomaton.getTransition(state, numTransitions-1, transition);
        if (idx >= term.bytes.length) {
          term.grow(1+idx);
        }
        //if (DEBUG) System.out.println("  push maxLabel=" + (char) lastTransition.max + " idx=" + idx);
        //System.out.println("  add trans dest=" + scratch.dest + " label=" + (char) scratch.max);
        term.bytes[idx] = (byte) transition.max;
        state = transition.dest;
        idx++;
      }
    }
  }

  // TODO: should this take startTerm too?  This way
  // Terms.intersect could forward to this method if type !=
  // NORMAL:
  public TermsEnum getTermsEnum(Terms terms) throws IOException {
    switch(type) {
    case NONE:
      return TermsEnum.EMPTY;
    case ALL:
      return terms.iterator(null);
    case SINGLE:
      return new SingleTermsEnum(terms.iterator(null), term);
    case PREFIX:
      // TODO: this is very likely faster than .intersect,
      // but we should test and maybe cutover
      return new PrefixTermsEnum(terms.iterator(null), term);
    case NORMAL:
      return terms.intersect(this, null);
    default:
      // unreachable
      throw new RuntimeException("unhandled case");
    }
  }

  /** Finds largest term accepted by this Automaton, that's
   *  <= the provided input term.  The result is placed in
   *  output; it's fine for output and input to point to
   *  the same BytesRef.  The returned result is either the
   *  provided output, or null if there is no floor term
   *  (ie, the provided input term is before the first term
   *  accepted by this Automaton). */
  public BytesRef floor(BytesRef input, BytesRef output) {

    output.offset = 0;
    //if (DEBUG) System.out.println("CA.floor input=" + input.utf8ToString());

    int state = runAutomaton.getInitialState();

    // Special case empty string:
    if (input.length == 0) {
      if (runAutomaton.isAccept(state)) {
        output.length = 0;
        return output;
      } else {
        return null;
      }
    }

    final List<Integer> stack = new ArrayList<>();

    int idx = 0;
    while (true) {
      int label = input.bytes[input.offset + idx] & 0xff;
      int nextState = runAutomaton.step(state, label);
      //if (DEBUG) System.out.println("  cycle label=" + (char) label + " nextState=" + nextState);

      if (idx == input.length-1) {
        if (nextState != -1 && runAutomaton.isAccept(nextState)) {
          // Input string is accepted
          if (idx >= output.bytes.length) {
            output.grow(1+idx);
          }
          output.bytes[idx] = (byte) label;
          output.length = input.length;
          //if (DEBUG) System.out.println("  input is accepted; return term=" + output.utf8ToString());
          return output;
        } else {
          nextState = -1;
        }
      }

      if (nextState == -1) {

        // Pop back to a state that has a transition
        // <= our label:
        while (true) {
          int numTransitions = lightAutomaton.getNumTransitions(state);
          if (numTransitions == 0) {
            assert runAutomaton.isAccept(state);
            output.length = idx;
            //if (DEBUG) System.out.println("  return " + output.utf8ToString());
            return output;
          } else {
            lightAutomaton.getTransition(state, 0, transition);

            if (label-1 < transition.min) {

              if (runAutomaton.isAccept(state)) {
                output.length = idx;
                //if (DEBUG) System.out.println("  return " + output.utf8ToString());
                return output;
              }
              // pop
              if (stack.size() == 0) {
                //if (DEBUG) System.out.println("  pop ord=" + idx + " return null");
                return null;
              } else {
                state = stack.remove(stack.size()-1);
                idx--;
                //if (DEBUG) System.out.println("  pop ord=" + (idx+1) + " label=" + (char) label + " first trans.min=" + (char) transitions[0].min);
                label = input.bytes[input.offset + idx] & 0xff;
              }
            } else {
              //if (DEBUG) System.out.println("  stop pop ord=" + idx + " first trans.min=" + (char) transitions[0].min);
              break;
            }
          }
        }

        //if (DEBUG) System.out.println("  label=" + (char) label + " idx=" + idx);

        return addTail(state, output, idx, label);
        
      } else {
        if (idx >= output.bytes.length) {
          output.grow(1+idx);
        }
        output.bytes[idx] = (byte) label;
        stack.add(state);
        state = nextState;
        idx++;
      }
    }
  }
  
  public String toDot() {
    StringBuilder b = new StringBuilder("digraph CompiledAutomaton {\n");
    b.append("  rankdir = LR;\n");
    int initial = 0;
    for (int i = 0; i < lightAutomaton.getNumStates(); i++) {
      b.append("  ").append(i);
      if (lightAutomaton.isAccept(i)) b.append(" [shape=doublecircle,label=\"\"];\n");
      else b.append(" [shape=circle,label=\"\"];\n");
      if (i == 0) {
        b.append("  initial [shape=plaintext,label=\"\"];\n");
        b.append("  initial -> ").append(i).append("\n");
      }
      int numTransitions = lightAutomaton.initTransition(i, transition);
      for (int j = 0; j < numTransitions; j++) {
        b.append("  ").append(i);
        b.append(" -> ");
        b.append(transition.dest);
        b.append(transition.min);
        if (transition.min != transition.max) {
          b.append("-");
          b.append(transition.max);
        }
        lightAutomaton.getNextTransition(transition);
      }
    }
    return b.append("}\n").toString();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((runAutomaton == null) ? 0 : runAutomaton.hashCode());
    result = prime * result + ((term == null) ? 0 : term.hashCode());
    result = prime * result + ((type == null) ? 0 : type.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    CompiledAutomaton other = (CompiledAutomaton) obj;
    if (type != other.type) return false;
    if (type == AUTOMATON_TYPE.SINGLE || type == AUTOMATON_TYPE.PREFIX) {
      if (!term.equals(other.term)) return false;
    } else if (type == AUTOMATON_TYPE.NORMAL) {
      if (!runAutomaton.equals(other.runAutomaton)) return false;
    }

    return true;
  }
}
