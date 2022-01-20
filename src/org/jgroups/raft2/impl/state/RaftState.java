package org.jgroups.raft2.impl.state;

import java.util.concurrent.locks.StampedLock;

import org.jgroups.raft2.impl.RaftId;
import org.jgroups.raft2.impl.RaftRole;

/**
 * TODO! document this
 *
 * @author Pedro Ruivo
 * @since 12
 */
public interface RaftState {

   void registerListener(RaftStateListener listener);

   RaftId getId();

   void setLeader(RaftId raftId);

   RaftId getLeader();

   StampedLock getStateLock();

   int getTerm();

   RaftRole getRole();

   void onNewTerm(int newTerm, RaftId leader);

   int changeToCandidate();

   void changeToLeader();





}
