package org.jgroups.raft2.impl.log;

/**
 * TODO! document this
 *
 * @author Pedro Ruivo
 * @since 12
 */
public interface RaftLogListener {

   void onNewEntry(int term, int logIndex);

   void onLeaderOldTerm(int term);

   void onLeaderAppendAccepted(int term, int lastLogIndex);

   void onLeaderAppendRejected(int term);



}
