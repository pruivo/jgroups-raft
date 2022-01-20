package org.jgroups.raft2;

import org.jgroups.util.SizeStreamable;

/**
 * TODO! document this
 *
 * @author Pedro Ruivo
 * @since 12
 */
public interface LogEntry extends SizeStreamable {

   int getTerm();

   int getLogIndex();

   default boolean isSame(int logIndex, int term) {
      return getLogIndex() == logIndex && getTerm() == term;
   }
}
