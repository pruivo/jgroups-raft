package org.jgroups.raft2.impl.state;

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.StampedLock;

import org.jgroups.raft2.impl.RaftId;
import org.jgroups.raft2.impl.RaftIdImpl;
import org.jgroups.raft2.impl.RaftRole;

/**
 * TODO! document this
 *
 * @author Pedro Ruivo
 * @since 12
 */
public class RaftStateImpl implements RaftState {

   private final Collection<RaftStateListener> listeners = new CopyOnWriteArrayList<>();
   private final StampedLock stateLock = new StampedLock();

   // currentTerm, votedFor & role guarded by termLock
   private int currentTerm;
   private RaftRole role = RaftRole.FOLLOWER;
   private RaftId leader;
   private final RaftId localRaftId;

   public RaftStateImpl(String raftId) {
      this.localRaftId = new RaftIdImpl(raftId);
   }

   public RaftStateImpl(RaftId raftId) {
      this.localRaftId = Objects.requireNonNull(raftId);
   }

   @Override
   public void registerListener(RaftStateListener listener) {
      listeners.add(listener);
   }

   @Override
   public RaftId getId() {
      return localRaftId;
   }

   @Override
   public StampedLock getStateLock() {
      return stateLock;
   }

   @Override
   public int getTerm() {
      return currentTerm;
   }

   @Override
   public RaftRole getRole() {
      return role;
   }

   @Override
   public void onNewTerm(int newTerm, RaftId leader) {
      assert newTerm > currentTerm;
      RaftEventBuilder builder = new RaftEventBuilder(this);
      currentTerm = newTerm;
      role = RaftRole.FOLLOWER;
      this.leader = leader;
      invokeListeners(builder.newTerm(newTerm).newRole(RaftRole.FOLLOWER).newLeader(leader).build());
   }

   @Override
   public int changeToCandidate() {
      RaftEventBuilder builder = new RaftEventBuilder(this);
      currentTerm++;
      role = RaftRole.CANDIDATE;
      leader = null;
      invokeListeners(builder.newTerm(currentTerm).newRole(RaftRole.CANDIDATE).newLeader(null).build());
      return currentTerm;
   }

   @Override
   public void changeToLeader() {
      RaftEventBuilder builder = new RaftEventBuilder(this);
      role = RaftRole.LEADER;
      leader = localRaftId;
      invokeListeners(builder.newRole(RaftRole.LEADER).newLeader(localRaftId).build());
   }

   @Override
   public void setLeader(RaftId raftId) {
      RaftEventBuilder builder = new RaftEventBuilder(this);
      leader = raftId;
      role = RaftRole.FOLLOWER;
      invokeListeners(builder.newRole(RaftRole.FOLLOWER).newLeader(raftId).build());
   }

   @Override
   public RaftId getLeader() {
      return leader;
   }

   private void invokeListeners(RaftStateEvent event) {
      for (RaftStateListener listener : listeners) {
         listener.onStateChangeEvent(event);
      }
   }

   private static class RaftEventBuilder {
      final int oldTerm;
      final RaftRole oldRole;
      final RaftId oldLeader;
      int newTerm;
      RaftRole newRole;
      RaftId newLeader;

      RaftEventBuilder(RaftStateImpl state) {
         oldTerm = state.getTerm();
         oldRole = state.getRole();
         oldLeader = state.getLeader();
         newTerm = oldTerm;
         newRole = oldRole;
         newLeader = oldLeader;
      }

      RaftEventBuilder newTerm(int term) {
         newTerm = term;
         return this;
      }

      RaftEventBuilder newRole(RaftRole role) {
         newRole = role;
         return this;
      }

      RaftEventBuilder newLeader(RaftId leader) {
         newLeader = leader;
         return this;
      }

      RaftStateEvent build() {
         return new RaftStateEventImpl(oldTerm, newTerm, oldRole, newRole, oldLeader, newLeader);
      }
   }
}
