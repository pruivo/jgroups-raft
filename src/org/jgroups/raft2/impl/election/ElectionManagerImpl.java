package org.jgroups.raft2.impl.election;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.StampedLock;

import org.jgroups.raft2.LogEntry;
import org.jgroups.raft2.impl.AbstractEventLoop;
import org.jgroups.raft2.impl.MessageSender;
import org.jgroups.raft2.impl.RaftId;
import org.jgroups.raft2.impl.RaftRole;
import org.jgroups.raft2.impl.cluster.ClusterConfiguration;
import org.jgroups.raft2.impl.cluster.VoteCollector;
import org.jgroups.raft2.impl.log.RaftLog;
import org.jgroups.raft2.impl.state.RaftState;
import org.jgroups.raft2.impl.state.RaftStateEvent;
import org.jgroups.raft2.impl.state.RaftStateListener;

/**
 * TODO! document this
 *
 * @author Pedro Ruivo
 * @since 12
 */
public class ElectionManagerImpl extends AbstractEventLoop implements ElectionManager, RaftStateListener {

   private static final AtomicReferenceFieldUpdater<ElectionManagerImpl, VoteRequestHeader> REQUEST_UPDATER = AtomicReferenceFieldUpdater.newUpdater(ElectionManagerImpl.class, VoteRequestHeader.class, "voteRequest");

   private final RaftState state;
   private final RaftLog raftLog;
   private final ClusterConfiguration cluster;
   private final MessageSender sender;

   // local state
   private volatile RaftId votedFor;
   private volatile VoteCollector voteCollector;

   // events
   private final BlockingQueue<VoteResponse> responses = new LinkedBlockingQueue<>();
   private volatile VoteRequestHeader voteRequest;

   public static ElectionManagerImpl create(Executor executor, RaftState state, RaftLog raftLog, ClusterConfiguration cluster, MessageSender sender) {
      ElectionManagerImpl e = new ElectionManagerImpl(executor, state, raftLog, cluster, sender);
      state.registerListener(e);
      return e;
   }

   private ElectionManagerImpl(Executor executor, RaftState state, RaftLog raftLog, ClusterConfiguration cluster, MessageSender sender) {
      super(executor);
      this.state = Objects.requireNonNull(state);
      this.raftLog = Objects.requireNonNull(raftLog);
      this.cluster = Objects.requireNonNull(cluster);
      this.sender = Objects.requireNonNull(sender);
   }

   @Override
   public void onElectionTimeout() {
      long stamp = state.getStateLock().writeLock();
      try {
         if (state.getRole() == RaftRole.LEADER) {
            //already a leader
            return;
         }
         int newTerm = state.changeToCandidate();

         voteCollector = cluster.newVoteCollector(newTerm);
         voteCollector.addVote(state.getId());

         votedFor = state.getId();
         LogEntry lastEntry = raftLog.getLast();
         REQUEST_UPDATER.set(this, new VoteRequestHeader(newTerm, state.getId(), lastEntry.getLogIndex(), lastEntry.getTerm()));
         triggerEvent();
      } finally {
         state.getStateLock().unlockWrite(stamp);
      }
   }

   @Override
   public void onVoteRequest(VoteRequestHeader header) {
      StampedLock lock = state.getStateLock();
      long stamp = lock.readLock();
      try {
         if (header.getTerm() < state.getTerm()) {
            // old term
            voteResponse(header.getCandidateId(), false);
            return;
         }
         if (header.getTerm() == state.getTerm() && votedFor != null) {
            voteResponse(header.getCandidateId(), votedFor.equals(header.getCandidateId()));
            return;
         }

         // acquire write lock to update the term
         long ws = lock.tryConvertToWriteLock(stamp);
         if (ws == 0) {
            // failed to convert the lock. release the read lock and acquire write lock
            lock.unlock(stamp);
            stamp = lock.writeLock();

            // double check nothing changed between releasing the read lock and acquires the write lock
            if (header.getTerm() < state.getTerm()) {
               return;
            }
         } else {
            stamp = ws;
         }

         // update the term if higher
         if (header.getTerm() > state.getTerm()) {
            state.onNewTerm(header.getTerm(), null);
            votedFor = null;
            voteCollector = null;
         }

         // if voted in current term
         if (votedFor != null) {
            voteResponse(header.getCandidateId(), votedFor.equals(header.getCandidateId()));
            return;
         }

         // check candidate's log
         LogEntry lastEntry = raftLog.getLast();
         if (header.getLastLogIdx() >= lastEntry.getLogIndex() && header.getLastLogTerm() >= lastEntry.getTerm()) {
            votedFor = header.getCandidateId();
            voteResponse(header.getCandidateId(), true);
            return;
         }

         voteResponse(header.getCandidateId(), false);
      } finally {
         lock.unlock(stamp);
      }
   }

   @Override
   public void onVoteResponse(VoteResponseHeader header) {
      StampedLock lock = state.getStateLock();
      long stamp = lock.readLock();
      try {
         if (header.getTerm() < state.getTerm() || state.getRole() != RaftRole.CANDIDATE) {
            // old term, discard
            return;
         }

         if (header.getTerm() == state.getTerm()) {
            VoteCollector collector = voteCollector;
            if (!header.isVoteGranted() || collector == null || !collector.addVote(header.getRaftId())) {
               // not collecting votes anymore, or not have a majority
               return;
            }
         }

         // write lock required
         // header.getTerm > current term or has majority
         long ws = lock.tryConvertToWriteLock(stamp);
         if (ws == 0) {
            lock.unlock(stamp);
            stamp = lock.writeLock();

            if (header.getTerm() < state.getTerm()) {
               // term changed and we have new leader
               return;
            }
         } else {
            stamp = ws;
         }

         // double check term didn't change
         if (header.getTerm() > state.getTerm()) {
            // change to follower
            state.onNewTerm(header.getTerm(), null);
            voteCollector = null;
            votedFor = null;
            return;
         }

         // vote majority
         state.changeToLeader();
         voteCollector = null;
      } finally {
         lock.unlock(stamp);
      }
   }

   @Override
   public void onStateChangeEvent(RaftStateEvent event) {
      if (event.isTermChanged()) {
         //new term
         votedFor = null;
         voteCollector = null;
      } else if (event.isRoleChanged() && event.getNewRole() != RaftRole.CANDIDATE) {
         // can be garbage collected
         voteCollector = null;
      }

   }

   @Override
   protected void onEvent() {
      List<VoteResponse> rspToSend = new ArrayList<>(responses.size());
      responses.drainTo(rspToSend);
      for (VoteResponse rsp : rspToSend) {
         sender.sendTo(rsp.candidateId, rsp.header);
      }
      VoteRequestHeader req = REQUEST_UPDATER.getAndSet(this, null);
      if (req != null) {
         sender.sendToAll(req);
      }
   }

   private void voteResponse(RaftId candidate, boolean voteGranted) {
      responses.add(new VoteResponse(candidate, new VoteResponseHeader(state.getTerm(), state.getId(), voteGranted)));
      triggerEvent();
   }

   // testing purposes
   public RaftId getVotedFor() {
      return votedFor;
   }

   // testing purposes
   public VoteCollector getVoteCollector() {
      return voteCollector;
   }

   private static class VoteResponse {
      final RaftId candidateId;
      final VoteResponseHeader header;


      private VoteResponse(RaftId candidateId, VoteResponseHeader header) {
         this.candidateId = candidateId;
         this.header = header;
      }
   }


}
