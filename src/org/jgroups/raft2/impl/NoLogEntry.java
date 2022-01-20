package org.jgroups.raft2.impl;

import org.jgroups.raft2.LogEntry;

/**
 * TODO! document this
 *
 * @author Pedro Ruivo
 * @since 12
 */
public enum NoLogEntry implements LogEntry {
   INSTANCE;

   @Override
   public int getTerm() {
      return 0;
   }

   @Override
   public int getLogIndex() {
      return 0;
   }
}
