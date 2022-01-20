package org.jgroups.raft2.impl.log;

import java.util.concurrent.Executor;

import org.jgroups.raft2.impl.AbstractEventLoop;

/**
 * TODO! document this
 *
 * @author Pedro Ruivo
 * @since 12
 */
public class PersistedRaftLogEventLoop extends AbstractEventLoop implements RaftLogListener {

   private volatile int lastPersistedLogIndex = -1;

   PersistedRaftLogEventLoop(Executor executor) {
      super(executor);
   }

   @Override
   protected void onEvent() {

   }

   @Override
   public void onNewEntry(int term, int logIndex) {

   }

   @Override
   public void onLeaderOldTerm(int term) {

   }

   @Override
   public void onLeaderAppendAccepted(int term, int lastLogIndex) {

   }

   @Override
   public void onLeaderAppendRejected(int term) {

   }
}
