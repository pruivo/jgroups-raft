package org.jgroups.raft2.impl.log;

import org.jgroups.raft2.LogEntry;

/**
 * TODO! document this
 *
 * @author Pedro Ruivo
 * @since 12
 */
public class EmptyLogEntry extends BaseLogEntry {

   public EmptyLogEntry(int term, int index) {
      super(term, index);
   }

}
