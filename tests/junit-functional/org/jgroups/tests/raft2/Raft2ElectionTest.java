package org.jgroups.tests.raft2;

import static org.testng.Assert.ThrowingRunnable;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import static org.testng.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.jgroups.Global;
import org.jgroups.View;
import org.jgroups.protocols.raft2.RaftConfigurationException;
import org.jgroups.raft2.ElectionTimeoutListener;
import org.jgroups.raft2.ElectionTimeoutTask;
import org.jgroups.raft2.LogEntry;
import org.jgroups.raft2.impl.Raft2Header;
import org.jgroups.raft2.impl.RaftId;
import org.jgroups.raft2.impl.RaftIdImpl;
import org.jgroups.raft2.impl.RaftRole;
import org.jgroups.raft2.impl.cluster.ClusterConfiguration;
import org.jgroups.raft2.impl.cluster.SimpleVoteCollector;
import org.jgroups.raft2.impl.cluster.VoteCollector;
import org.jgroups.raft2.impl.election.ElectionManagerImpl;
import org.jgroups.raft2.impl.election.ElectionTimeoutTaskImpl;
import org.jgroups.raft2.impl.election.VoteRequestHeader;
import org.jgroups.raft2.impl.election.VoteResponseHeader;
import org.jgroups.raft2.impl.log.EmptyLogEntry;
import org.jgroups.raft2.impl.log.RaftLog;
import org.jgroups.raft2.impl.state.RaftState;
import org.jgroups.raft2.impl.state.RaftStateImpl;
import org.jgroups.tests.utils.QueueMessageSender;
import org.jgroups.tests.utils.WithinThreadExecutor;
import org.testng.annotations.Test;

/**
 * TODO! document this
 *
 * @author Pedro Ruivo
 * @since 12
 */
@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class Raft2ElectionTest {

   @Test
   public void testHeartBeatResetTimer() throws RaftConfigurationException {
      ElectionTimeoutTask task = ElectionTimeoutTaskImpl.create(1, 2);
      TestElectionListener listener = new TestElectionListener();
      task.add(listener);

      // simulate a timeout
      task.run();

      assertTrue(listener.timedOut);
      listener.timedOut = false;

      // simulates heartbeat received
      task.onMessageReceived();
      task.run();

      assertFalse(listener.timedOut);
   }

   @Test
   public void testElectionConfiguration() {
      // min is zero
      assertRaftConfigurationException("minTimeoutMillis (0) must be greater than zero", () -> ElectionTimeoutTaskImpl.create(0, 1));
      assertRaftConfigurationException("minTimeoutMillis (-1) must be greater than zero", () -> ElectionTimeoutTaskImpl.create(-1, 1));

      assertRaftConfigurationException("maxTimeoutMillis (0) must be greater than zero", () -> ElectionTimeoutTaskImpl.create(1, 0));
      assertRaftConfigurationException("maxTimeoutMillis (-1) must be greater than zero", () -> ElectionTimeoutTaskImpl.create(1, -1));
      assertRaftConfigurationException("minTimeoutMillis (10) must be lower than maxTimeoutMillis (10)", () -> ElectionTimeoutTaskImpl.create(10, 10));

      try {
         ElectionTimeoutTaskImpl.create(1, 2);
      } catch (RaftConfigurationException e) {
         fail("No exception expected: " + e.getMessage());
      }
   }

   @Test
   public void testElection() throws InterruptedException {
      List<RaftId> members = Arrays.asList(new RaftIdImpl("a"), new RaftIdImpl("b"), new RaftIdImpl("c"));

      // only need 2 nodes (== majority) for this test
      RaftNode node0 = new RaftNode(members, 0);
      RaftNode node1 = new RaftNode(members, 1);

      int initialTerm = 5;
      int lastLogIdx = 31;

      for (RaftNode n : Arrays.asList(node0, node1)) {
         n.setTerm(initialTerm);
         n.setLastLogEntry(initialTerm, lastLogIdx);
         n.assertRoleAndTerm(initialTerm, RaftRole.FOLLOWER);
      }

      // timeout, should change state to candidate
      node0.electionManager.onElectionTimeout();
      node0.assertRoleAndTerm(initialTerm + 1, RaftRole.CANDIDATE);
      node0.assertVotedFor(members.get(0));

      QueueMessageSender.QueuedMessage msg = node0.sender.poll(10, TimeUnit.SECONDS);
      assertNotNull(msg);
      assertNull(msg.getDest());
      assertVoteRequest(msg.getHeader(), members.get(0), initialTerm + 1, lastLogIdx, initialTerm);

      // node1 updates the term and sends the response
      node1.electionManager.onVoteRequest((VoteRequestHeader) msg.getHeader());
      node1.assertRoleAndTerm(initialTerm + 1, RaftRole.FOLLOWER);
      assertEquals(node1.electionManager.getVotedFor(), members.get(0));

      msg = node1.sender.poll(10, TimeUnit.SECONDS);
      assertNotNull(msg);
      assertEquals(msg.getDest(), members.get(0));
      assertVoteResponse(msg.getHeader(), initialTerm + 1, members.get(1), true);


      node0.electionManager.onVoteResponse((VoteResponseHeader) msg.getHeader());
      node0.assertRoleAndTerm(initialTerm + 1, RaftRole.LEADER);
      assertNull(node0.electionManager.getVoteCollector());
   }

   @Test
   public void testConcurrentElection() throws InterruptedException {
      List<RaftId> members = Arrays.asList(new RaftIdImpl("a"), new RaftIdImpl("b"), new RaftIdImpl("c"));
      List<RaftNode> nodes = new ArrayList<>(3);

      int initialTerm = 5;
      int lastLogIdx = 31;

      for (int i = 0; i < 3; ++i) {
         RaftNode n = new RaftNode(members, i);
         n.setTerm(initialTerm);
         n.setLastLogEntry(initialTerm, lastLogIdx);
         n.assertRoleAndTerm(initialTerm, RaftRole.FOLLOWER);
         nodes.add(n);
      }

      // timeout on nodes 0 & 1
      for (int i = 0; i < 2; ++i) {
         nodes.get(i).electionManager.onElectionTimeout();
         nodes.get(i).assertRoleAndTerm(initialTerm + 1, RaftRole.CANDIDATE);
         nodes.get(i).assertVotedFor(members.get(i));
      }

      // get vote request from node0
      QueueMessageSender.QueuedMessage req0 = nodes.get(0).sender.poll(10, TimeUnit.SECONDS);
      assertNotNull(req0);
      assertNull(req0.getDest());
      assertVoteRequest(req0.getHeader(), members.get(0), initialTerm + 1, lastLogIdx, initialTerm);

      // get vote request from node1
      QueueMessageSender.QueuedMessage req1 = nodes.get(1).sender.poll(10, TimeUnit.SECONDS);
      assertNotNull(req1);
      assertNull(req1.getDest());
      assertVoteRequest(req1.getHeader(), members.get(1), initialTerm + 1, lastLogIdx, initialTerm);

      CyclicBarrier barrier = new CyclicBarrier(2);
      CompletableFuture<Void> f1 = CompletableFuture.runAsync(new VoteRequestRunnable(nodes.get(2), (VoteRequestHeader) req0.getHeader(), barrier));
      CompletableFuture<Void> f2 = CompletableFuture.runAsync(new VoteRequestRunnable(nodes.get(2), (VoteRequestHeader) req1.getHeader(), barrier));

      // wait for it
      f1.join();
      f2.join();

      // we should have 2 responses
      QueueMessageSender.QueuedMessage res2_1 = nodes.get(2).sender.poll(10, TimeUnit.SECONDS);
      QueueMessageSender.QueuedMessage res2_2 = nodes.get(2).sender.poll(10, TimeUnit.SECONDS);

      nodes.get(2).assertRoleAndTerm(initialTerm + 1, RaftRole.FOLLOWER);

      assertNotNull(res2_1);
      assertNotNull(res2_2);
      assertNotEquals(res2_1.getDest(), res2_2.getDest());

      RaftId leader = assertOnlyOneVoteGranted(Arrays.asList(res2_1, res2_2), initialTerm + 1, members.get(2));

      // node0 receives vote request from node1 and vice versa
      nodes.get(0).electionManager.onVoteRequest((VoteRequestHeader) req1.getHeader());
      nodes.get(1).electionManager.onVoteRequest((VoteRequestHeader) req0.getHeader());

      // both vote request must not be granted
      assertVoteResponse(nodes.get(0).sender.poll(10, TimeUnit.SECONDS), members.get(0), members.get(1), initialTerm + 1, false);
      assertVoteResponse(nodes.get(1).sender.poll(10, TimeUnit.SECONDS), members.get(1), members.get(0), initialTerm + 1, false);

      // apply node2 response to node0 and node1
      for (QueueMessageSender.QueuedMessage rsp : Arrays.asList(res2_1, res2_2)) {
         VoteResponseHeader header = (VoteResponseHeader) rsp.getHeader();
         if (members.get(0).equals(rsp.getDest())) {
            nodes.get(0).electionManager.onVoteResponse(header);
         } else if (members.get(1).equals(rsp.getDest())) {
            nodes.get(1).electionManager.onVoteResponse(header);
         } else {
            throw new IllegalStateException();
         }
      }

      // at the end, only one of them is a leader and the other is a candidate
      for (int i = 0; i < 2; ++i) {
         if (members.get(i).equals(leader)) {
            nodes.get(i).assertRoleAndTerm(initialTerm + 1, RaftRole.LEADER);
            assertNull(nodes.get(i).electionManager.getVoteCollector());
         } else {
            // stays in CANDIDATE until a heartbeat is received.
            nodes.get(i).assertRoleAndTerm(initialTerm + 1, RaftRole.CANDIDATE);
         }
      }
   }

   @Test
   public void testShortLogElection() throws InterruptedException {
      doDifferentLogTest(0, false, Arrays.asList(true, false, false));
   }

   @Test
   public void testMiddleLogElection() throws InterruptedException {
      doDifferentLogTest(1, true, Arrays.asList(true, true, false));
   }

   @Test
   public void testLongLogElection() throws InterruptedException {
      doDifferentLogTest(2, true, Arrays.asList(true, true, true));
   }

   private void doDifferentLogTest(int candidateIdx, boolean becomeLeader, List<Boolean> votes) throws InterruptedException {
      List<RaftId> members = Arrays.asList(new RaftIdImpl("a"), new RaftIdImpl("b"), new RaftIdImpl("c"));
      List<RaftNode> nodes = new ArrayList<>(3);

      int initialTerm = 5;
      int lastLogIdx = 31;

      for (int i = 0; i < 3; ++i) {
         RaftNode n = new RaftNode(members, i);
         n.setTerm(initialTerm);
         n.setLastLogEntry(initialTerm, lastLogIdx + i);
         n.assertRoleAndTerm(initialTerm, RaftRole.FOLLOWER);
         nodes.add(n);
      }

      nodes.get(candidateIdx).electionManager.onElectionTimeout();
      nodes.get(candidateIdx).assertRoleAndTerm(initialTerm + 1, RaftRole.CANDIDATE);
      nodes.get(candidateIdx).assertVotedFor(members.get(candidateIdx));

      QueueMessageSender.QueuedMessage req = nodes.get(candidateIdx).sender.poll(10, TimeUnit.SECONDS);
      assertVoteRequest(req, members.get(candidateIdx), initialTerm + 1, lastLogIdx + candidateIdx, initialTerm);

      for (int i = 0; i < 3; ++i) {
         if (i == candidateIdx) {
            continue;
         }
         nodes.get(i).electionManager.onVoteRequest((VoteRequestHeader) req.getHeader());
         nodes.get(i).assertRoleAndTerm(initialTerm + 1, RaftRole.FOLLOWER);
         QueueMessageSender.QueuedMessage rsp = nodes.get(i).sender.poll(10, TimeUnit.SECONDS);
         assertVoteResponse(rsp, members.get(i), members.get(candidateIdx), initialTerm + 1, votes.get(i));
         nodes.get(candidateIdx).electionManager.onVoteResponse((VoteResponseHeader) rsp.getHeader());
      }

      for (int i = 0; i < 3; ++i) {
         if (i == candidateIdx) {
            nodes.get(i).assertRoleAndTerm(initialTerm + 1, becomeLeader ? RaftRole.LEADER : RaftRole.CANDIDATE);
         } else {
            nodes.get(i).assertRoleAndTerm(initialTerm + 1, RaftRole.FOLLOWER);
         }
      }
   }

   private void assertVoteRequest(QueueMessageSender.QueuedMessage message, RaftId candidate, int term, int lastLogIdx, int lastLogTerm) {
      assertNotNull(message);
      assertNull(message.getDest());
      assertVoteRequest(message.getHeader(), candidate, term, lastLogIdx, lastLogTerm);
   }

   private void assertVoteRequest(Raft2Header header, RaftId candidate, int term, int lastLogIdx, int lastLogTerm) {
      assertEquals(header.getClass(), VoteRequestHeader.class);
      VoteRequestHeader req = (VoteRequestHeader) header;
      assertEquals(req.getCandidateId(), candidate);
      assertEquals(req.getTerm(), term);
      assertEquals(req.getLastLogIdx(), lastLogIdx);
      assertEquals(req.getLastLogTerm(), lastLogTerm);
   }

   private void assertVoteResponse(QueueMessageSender.QueuedMessage rsp, RaftId from, RaftId to, int term, boolean voteGranted) {
      assertNotNull(rsp);
      assertEquals(rsp.getDest(), to);
      assertVoteResponse(rsp.getHeader(), term, from, voteGranted);
   }

   private void assertVoteResponse(Raft2Header header, int term, RaftId from, boolean voteGranted) {
      assertEquals(VoteResponseHeader.class, header.getClass());
      VoteResponseHeader res = (VoteResponseHeader) header;
      assertEquals(res.getTerm(), term);
      assertEquals(res.getRaftId(), from);
      assertEquals(res.isVoteGranted(), voteGranted);
   }

   private RaftId assertOnlyOneVoteGranted(Collection<QueueMessageSender.QueuedMessage> responses, int term, RaftId from) {
      int votesGranted = 0;
      RaftId voteGrantedTo = null;
      for (QueueMessageSender.QueuedMessage m : responses) {
         assertTrue(m.getHeader() instanceof VoteResponseHeader);
         VoteResponseHeader res = (VoteResponseHeader) m.getHeader();
         assertEquals(res.getTerm(), term);
         assertEquals(res.getRaftId(), from);
         if (res.isVoteGranted()) {
            votesGranted++;
            voteGrantedTo = m.getDest();
         }
      }
      // expects a single vote granted
      assertEquals(votesGranted, 1);
      return voteGrantedTo;
   }

   private void assertRaftConfigurationException(String message, ThrowingRunnable runnable) {
      RaftConfigurationException e = expectThrows(RaftConfigurationException.class, runnable);
      assertEquals(e.getMessage(), message);
   }

   private static class TestElectionListener implements ElectionTimeoutListener {

      volatile boolean timedOut;

      @Override
      public void onElectionTimeout() {
         timedOut = true;
      }
   }

   private static class TestClusterConfiguration implements ClusterConfiguration {

      private final Collection<RaftId> cluster;

      private TestClusterConfiguration(Collection<RaftId> cluster) {
         this.cluster = cluster;
      }

      @Override
      public VoteCollector newVoteCollector(int term) {
         return new SimpleVoteCollector(term, cluster);
      }

      @Override
      public void handleView(View newView) {
         //no-op
      }

      @Override
      public Collection<RaftId> cluster() {
         return cluster;
      }
   }

   private static class TestRaftLog implements RaftLog {

      volatile int lastTerm;
      volatile int lastIdx;

      @Override
      public LogEntry append(byte[] data, int offset, int length) {
         return null;
      }

      @Override
      public LogEntry getLast() {
         return new EmptyLogEntry(lastTerm, lastIdx);
      }
   }

   private static class RaftNode {
      final TestClusterConfiguration cluster;
      final RaftState state;
      final QueueMessageSender sender = new QueueMessageSender();
      final TestRaftLog raftLog = new TestRaftLog();
      final ElectionManagerImpl electionManager;

      RaftNode(List<RaftId> members, int localIdx) {
         cluster = new TestClusterConfiguration(members);
         state = new RaftStateImpl(members.get(localIdx));
         electionManager = ElectionManagerImpl.create(WithinThreadExecutor.INSTANCE, state, raftLog, cluster, sender);
      }

      void setTerm(int term) {
         long stamp = state.getStateLock().writeLock();
         try {
            state.onNewTerm(term, null);
         } finally {
            state.getStateLock().unlockWrite(stamp);
         }
      }

      void setLastLogEntry(int term, int index) {
         raftLog.lastTerm = term;
         raftLog.lastIdx = index;
      }

      void assertRoleAndTerm(int term, RaftRole role) {
         long stamp = state.getStateLock().readLock();
         try {
            assertEquals(state.getTerm(), term);
            assertEquals(state.getRole(), role);
         } finally {
            state.getStateLock().unlockRead(stamp);
         }
      }

      void assertVotedFor(RaftId raftId) {
         assertEquals(electionManager.getVotedFor(), raftId);
      }
   }

   private static class VoteRequestRunnable implements Runnable {

      private final RaftNode receiver;
      private final VoteRequestHeader header;
      private final CyclicBarrier barrier;

      private VoteRequestRunnable(RaftNode receiver, VoteRequestHeader header, CyclicBarrier barrier) {
         this.receiver = receiver;
         this.header = header;
         this.barrier = barrier;
      }

      @Override
      public void run() {
         try {
            barrier.await(10, TimeUnit.SECONDS);
         } catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
            fail();
         }
         receiver.electionManager.onVoteRequest(header);
      }
   }
}
