package org.jgroups.raft2.impl.log;

import org.jgroups.raft2.LogEntry;

/**
 * TODO! document this
 *
 * @author Pedro Ruivo
 * @since 12
 */
public interface PersistedRaftLog {

   void persistEntry(LogEntry entry);

}
