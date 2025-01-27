 package org.jgroups.tests;

 import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.protocols.DISCARD;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.raft.*;
import org.jgroups.raft.RaftHandle;
import org.jgroups.raft.blocks.ReplicatedStateMachine;
 import org.jgroups.raft.util.Utils;
 import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
 import java.util.stream.Collectors;
 import java.util.stream.Stream;

import static org.testng.Assert.*;

/**
 * Tests the AppendEntries functionality: appending log entries in regular operation, new members, late joiners etc
 * @author Bela Ban
 * @since  0.2
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class AppendEntriesTest {
    // A is always the leader because started first and logs are equal
    protected JChannel                                a,  b,  c;
    protected ReplicatedStateMachine<Integer,Integer> as, bs, cs;
    protected static final String                     CLUSTER="AppendEntriesTest";
    protected final List<String>                      members=Arrays.asList("A", "B", "C");
    protected static final long                       TIMEOUT=5000, INTERVAL=100;
    protected static final Address                    leader=Util.createRandomAddress("A");
    protected static final byte[]                     buf=new byte[10];
    protected static final int[]                      terms={0,1,1,1,4,4,5,5,6,6,6};


    @AfterMethod
    protected void destroy() {
        close(c, b, a);
    }

    public void testSingleMember() throws Exception  {
        // given
        a=create("A", Collections.singletonList("A"));
        a.connect(CLUSTER);
        RaftHandle raft=new RaftHandle(a, new DummyStateMachine());
        Util.waitUntil(1000, 250, raft::isLeader);

        // when
        byte[] data="foo".getBytes();
        byte[] result=raft.set(data, 0, data.length, 1, TimeUnit.SECONDS);

        // then
        assert Arrays.equals(result, new byte[0]) : "should have received empty byte array";
    }

    public void testNormalOperation() throws Exception {
        init(true);
        for(int i=1; i <= 10; i++)
            as.put(i, i);
        assertSame(as, bs, cs);
        bs.remove(5);
        cs.put(11, 11);
        cs.remove(1);
        as.put(1, 1);
        assertSame(as, bs, cs);
    }


    public void testRedirect() throws Exception {
        init(true);
        cs.put(5, 5);
        assertSame(as, bs, cs);
    }


    public void testPutWithoutLeader() throws Exception {
        a=create("A"); // leader
        as=new ReplicatedStateMachine<>(a);
        a.connect(CLUSTER);
        assert !isLeader(a);
        try {
            as.put(1, 1);
            assert false : "put() should fail as we don't have a leader";
        }
        catch(Throwable t) {
            System.out.println("received exception as expected: " + t);
        }
    }


    /**
     * The leader has no majority in the cluster and therefore cannot commit changes. The effect is that all puts on the
     * state machine will fail and the replicated state machine will be empty.
     */
    public void testNonCommitWithoutMajority() throws Exception {
        init(true);
        close(b, c);
        as.timeout(500);

        for(int i=1; i <= 3; i++) {
            try {
                as.put(i, i);
            }
            catch(Exception ex) {
                System.out.printf("received %s as expected; cache size is %d\n", ex.getClass().getSimpleName(), as.size());
            }
            assert as.size() == 0;
        }
    }

    /**
     * Leader A and followers B and C commit entries 1-2. Then C leaves and A and B commit entries 3-5. When C rejoins,
     * it should get log entries 3-5 as well.
     */
    public void testCatchingUp() throws Exception {
        init(true);
        // A, B and C commit entries 1-2
        for(int i=1; i <= 2; i++)
            as.put(i,i);
        assertSame(as, bs, cs);

        // Now C leaves
        close(c);

        // A and B commit entries 3-5
        for(int i=3; i <= 5; i++)
            as.put(i,i);
        assertSame(as, bs);

        // Now start C again: entries 1-5 will have to get resent to C as its log was deleted above (otherwise only 3-5
        // would have to be resent)
        System.out.println("-- starting C again, needs to catch up");
        c=create("C");  // follower
        cs=new ReplicatedStateMachine<>(c);
        c.connect(CLUSTER);
        Util.waitUntilAllChannelsHaveSameView(5000, 100, a,b,c);

        // Now C should also have the same entries (1-5) as A and B
        raft(a).resendInterval(200);
        assertSame(as, bs, cs);
    }

    /**
     * Leader A adds the first entry to its log but cannot commit it because it doesn't have a majority (B and C
     * discard all traffic). Then, the DISCARD protocol is removed and the commit entries of B and C should catch up.
     */
    public void testCatchingUpFirstEntry() throws Exception {
        // Create {A,B,C}, A is the leader, then close B and C (A is still the leader)
        init(false);

        // make B and C drop all traffic; this means A won't be able to commit
        for(JChannel ch: Arrays.asList(b,c)) {
            ProtocolStack stack=ch.getProtocolStack();
            DISCARD discard=new DISCARD().discardAll(true).setAddress(ch.getAddress());
            stack.insertProtocol(discard, ProtocolStack.Position.ABOVE, TP.class);
        };

        // Add the first entry, this will time out as there's no majority
        as.timeout(500);
        try {
            as.put(1, 1);
            assert false : "should have gotten a TimeoutException";
        }
        catch(TimeoutException ignored) {
            System.out.println("The first put() timed out as expected as there's no majority to commit it");
        }

        RAFT raft=a.getProtocolStack().findProtocol(RAFT.class);
        System.out.printf("A: last-applied=%d, commit-index=%d\n", raft.lastAppended(), raft.commitIndex());
        assert raft.lastAppended() == 1;
        assert raft.commitIndex() == 0;

        // now remove the DISCARD protocol from B and C
        Stream.of(b,c).forEach(ch -> ch.getProtocolStack().removeProtocol(DISCARD.class));

        assertCommitIndex(10000, 500, raft.lastAppended(), raft.lastAppended(), a, b);
        for(JChannel ch: Arrays.asList(a,b)) {
            raft=ch.getProtocolStack().findProtocol(RAFT.class);
            System.out.printf("%s: last-applied=%d, commit-index=%d\n", ch.getAddress(), raft.lastAppended(), raft.commitIndex());
            assert raft.lastAppended() == 1;
            assert raft.commitIndex() == 1;
        }
        assertSame(as, bs, cs);
    }

    /**
     * Tests https://github.com/belaban/jgroups-raft/issues/30-31: correct commit_index after leader restart, and
     * populating request table in RAFT on leader change. Note that the commit index should still be 0, as the change
     * on the leader should throw an exception!
     */
    public void testLeaderRestart() throws Exception {
        a=create("A");
        raft(a).stateMachine(new DummyStateMachine());
        b=create("B");
        raft(b).stateMachine(new DummyStateMachine());
        a.connect(CLUSTER);
        b.connect(CLUSTER);
        // A and B now have a majority and A is leader
        Util.waitUntilAllChannelsHaveSameView(10000, 500, a, b);

        assertLeader(a, 10000, 500);
        assert !raft(b).isLeader();
        System.out.println("--> disconnecting B");
        b.disconnect(); // stop B; it was only needed to make A the leader
        Util.waitUntil(5000, 100, () -> !raft(a).isLeader());

        // Now try to make a change on A. This will fail as A is not leader anymore
        try {
            raft(a).set(new byte[]{'b', 'e', 'l', 'a'}, 0, 4, 500, TimeUnit.MILLISECONDS);
            assert false : "set() should have thrown a timeout as we cannot commit the change";
        }
        catch(IllegalStateException ex) {
            System.out.printf("got exception as expected: %s\n", ex);
        }

        // A now has last_applied=1 and commit_index=0:
        assertCommitIndex(10000, 500, 0, 0, a);

        // Now start B again, this gives us a majority and entry #1 should be able to be committed
        System.out.println("--> restarting B");
        b=create("B");
        raft(b).stateMachine(new DummyStateMachine());
        b.connect(CLUSTER);
        // A and B now have a majority and A is leader
        Util.waitUntilAllChannelsHaveSameView(10000, 500, a, b);

        // A and B should now have last_applied=0 and commit_index=0
        assertCommitIndex(10000, 500, 0, 0, a,b);
    }


    /**
     * Leader A and follower B commit 5 entries, then snapshot A. Then C comes up and should get the 5 committed entries
     * as well, as a snapshot
     */
    public void testInstallSnapshotInC() throws Exception {
        init(true);
        close(c);
        for(int i=1; i <= 5; i++)
            as.put(i,i);
        assertSame(as, bs);

        // Snapshot A:
        as.snapshot();

        // Now start C
        c=create("C");  // follower
        cs=new ReplicatedStateMachine<>(c);
        c.connect(CLUSTER);
        Util.waitUntilAllChannelsHaveSameView(10000, 500, a, b, c);

        assertSame(as, bs, cs);
    }


    /** Tests an append at index 1 with prev_index 0 and index=2 with prev_index=1 */
    public void testInitialAppends() throws Exception {
        initB();
        RaftImpl impl=getImpl(b);
        Log log=impl.raft().log();

        AppendResult result=append(impl, 1, 0, new LogEntry(4, buf), leader, 1);
        assert result.success();
        assertEquals(result.index(), 1);
        assertEquals(result.commitIndex(), 1);
        assertLogIndices(log, 1, 1, 4);

        result=append(impl, 2, 4, new LogEntry(4, buf), leader, 1);
        assert result.success();
        assertEquals(result.index(), 2);
        assertEquals(result.commitIndex(), 1);
        assertLogIndices(log, 2, 1, 4);

        result=append(impl, 2, 4, new LogEntry(4, null), leader, 2);
        assert result.success();
        assertEquals(result.index(), 2);
        assertEquals(result.commitIndex(), 2);
        assertLogIndices(log, 2, 2, 4);
    }

    /** Tests append _after_ the last appended index; returns an AppendResult with index=last_appended */
    public void testAppendAfterLastAppened() throws Exception {
        initB();
        RaftImpl impl=getImpl(b);
        Log log=impl.raft().log();

        // initial append at index 1
        AppendResult result=append(impl, 1, 0, new LogEntry(4, buf), leader, 1);
        assert result.success();
        assertEquals(result.index(), 1);
        assertLogIndices(log, 1, 1, 4);

        // append at index 3 fails because there is no entry at index 2
        result=append(impl, 3, 4, new LogEntry(4, buf), leader, 1);
        assert !result.success();
        assertEquals(result.index(), 1);
        assertLogIndices(log, 1, 1, 4);
    }



    public void testSendCommitsImmediately() throws Exception {
        // Force leader to send commit messages immediately
        init(true, r -> r.resendInterval(60_000).sendCommitsImmediately(true));
        as.put(1,1);
        assertSame(as, bs, cs);
    }

    // Index  01 02 03 04 05 06 07 08 09 10 11 12
    // Log    01 01 01 04 04 05 05 06 06 06
    // Append                            07 <--- wrong prev_term at index 11
    public void testAppendWithConflictingTerm() throws Exception {
        initB();
        RaftImpl impl=getImpl(b);
        Log log=impl.raft().log();

        for(int i=1; i <= 10; i++)
            append(impl,  i, terms[i-1], new LogEntry(terms[i], buf), leader, 1);

        // now append(index=11,term=7) -> should return false result with index=8
        AppendResult result=append(impl, 11, 7, new LogEntry(6, buf), leader, 1);
        assert !result.success();
        assertEquals(result.index(), 8);
        assertEquals(result.nonMatchingTerm(), 6);
        assertLogIndices(log, 7, 1, 5);
    }


    // Index  01 02 03 04 05 06 07 08 09 10 11 12
    // Log    01
    // Append    07 <--- wrong prev_term at index 1
    public void testAppendWithConflictingTerm2() throws Exception {
        initB();
        RaftImpl impl=getImpl(b);
        Log log=impl.raft().log();

        append(impl,  1, 0, new LogEntry(1, buf), leader, 0);

        // now append(index=2,term=7) -> should return false result with index=1
        AppendResult result=append(impl, 2, 7, new LogEntry(7, buf), leader, 1);
        assert !result.success();
        assertEquals(result.index(), 1);
        assertEquals(result.nonMatchingTerm(), 1);
        assertLogIndices(log, 0, 0, 0);
    }


    // Index  01 02 03 04 05 06 07 08 09 10 11 12
    // Log    01 03 05
    // Append          07 <--- wrong prev_term at index 3
    public void testAppendWithConflictingTerm3() throws Exception {
        initB();
        RaftImpl impl=getImpl(b);
        Log log=impl.raft().log();

        append(impl,  1, 0, new LogEntry(1, buf), leader, 1);
        append(impl,  2, 1, new LogEntry(3, buf), leader, 1);
        append(impl,  3, 3, new LogEntry(5, buf), leader, 1);

        // now append(index=2,term=7) -> should return false result with index=1
        AppendResult result=append(impl, 4, 7, new LogEntry(7, buf), leader, 1);
        assert !result.success();
        assertEquals(result.index(), 3);
        assertEquals(result.nonMatchingTerm(), 5);
        assertLogIndices(log, 2, 1, 3);
    }


    // Index  01 02 03 04 05 06 07 08 09 10 11 12
    // Leader 01 01 01 04 04 05 05 06 06 06
    public void testRAFTPaperAppendOnLeader() throws Exception {
        initB();
        RaftImpl impl=getImpl(b);
        Log log=impl.raft().log();
        append(impl,  1, 0, new LogEntry(1, buf), leader, 1);
        append(impl,  2, 1, new LogEntry(1, buf), leader, 1);
        append(impl,  3, 1, new LogEntry(1, buf), leader, 1);
        append(impl,  4, 1, new LogEntry(4, buf), leader, 1);
        append(impl,  5, 4, new LogEntry(4, buf), leader, 1);
        append(impl,  6, 4, new LogEntry(5, buf), leader, 1);
        append(impl,  7, 5, new LogEntry(5, buf), leader, 1);
        append(impl,  8, 5, new LogEntry(6, buf), leader, 1);
        append(impl,  9, 6, new LogEntry(6, buf), leader, 1);
        append(impl, 10, 6, new LogEntry(6, buf), leader, 10);
        AppendResult result=append(impl, 11, 6, new LogEntry(6, buf), leader, 1);
        assertTrue(result.success());
        assertEquals(result.index(), 11);
        assertLogIndices(log, 11, 10, 6);
    }

    // 5.3, fig. 7
    // Index  01 02 03 04 05 06 07 08 09 10 11 12
    // Leader 01 01 01 04 04 05 05 06 06 06
    // Flwr A 01 01 01 04 04 05 05 06 06    06 <-- add
    public void testRAFTPaperScenarioA() throws Exception {
        initB();
        RaftImpl impl=getImpl(b);
        Log log=impl.raft().log();
        append(impl,  1, 0, new LogEntry(1, buf), leader, 1);
        append(impl,  2, 1, new LogEntry(1, buf), leader, 1);
        append(impl,  3, 1, new LogEntry(1, buf), leader, 1);
        append(impl,  4, 1, new LogEntry(4, buf), leader, 1);
        append(impl,  5, 4, new LogEntry(4, buf), leader, 1);
        append(impl,  6, 4, new LogEntry(5, buf), leader, 1);
        append(impl,  7, 5, new LogEntry(5, buf), leader, 1);
        append(impl,  8, 5, new LogEntry(6, buf), leader, 1);
        append(impl,  9, 6, new LogEntry(6, buf), leader, 9);
        AppendResult result=append(impl, 11, 6, new LogEntry(6, buf), leader, 9);
        assertFalse(result.success());
        assertEquals(result.index(), 9);
        assertLogIndices(log, 9, 9, 6);
    }


    // 5.3, fig. 7
    // Index  01 02 03 04 05 06 07 08 09 10 11 12
    // Leader 01 01 01 04 04 05 05 06 06 06
    // Flwr A 01 01 01 04                   06 <-- add
    public void testRAFTPaperScenarioB() throws Exception {
        initB();
        RaftImpl impl=getImpl(b);
        Log log=impl.raft().log();
        append(impl,  1, 0, new LogEntry(1, buf), leader, 1);
        append(impl, 2, 1, new LogEntry(1, buf), leader, 1);
        append(impl, 3, 1, new LogEntry(1, buf), leader, 1);
        append(impl, 4, 1, new LogEntry(4, buf), leader, 4);
        AppendResult result=append(impl, 11, 6, new LogEntry(6, buf), leader, 4);
        assertFalse(result.success());
        assertEquals(result.index(), 4);
        assertLogIndices(log, 4, 4, 4);
    }

    // 5.3, fig. 7
    // Index  01 02 03 04 05 06 07 08 09 10 11 12
    // Leader 01 01 01 04 04 05 05 06 06 06
    // Flwr A 01 01 01 04 04 05 05 06 06 06 06
    public void testRAFTPaperScenarioC() throws Exception {
        initB();
        RaftImpl impl=getImpl(b);
        Log log=impl.raft().log();
        append(impl,  1, 0, new LogEntry(1, buf), leader, 1);
        append(impl,  2, 1, new LogEntry(1, buf), leader, 1);
        append(impl,  3, 1, new LogEntry(1, buf), leader, 1);
        append(impl,  4, 1, new LogEntry(4, buf), leader, 1);
        append(impl,  5, 4, new LogEntry(4, buf), leader, 1);
        append(impl,  6, 4, new LogEntry(5, buf), leader, 1);
        append(impl,  7, 5, new LogEntry(5, buf), leader, 1);
        append(impl,  8, 5, new LogEntry(6, buf), leader, 1);
        append(impl,  9, 6, new LogEntry(6, buf), leader, 1);
        append(impl, 10, 6, new LogEntry(6, buf), leader, 1);
        append(impl, 11, 6, new LogEntry(6, buf), leader, 10);
        // Overwrites existing entry; does *not* advance last_applied in log
        AppendResult result=append(impl, 11, 6, new LogEntry(6, buf), leader, 10);
        assertTrue(result.success());
        assertEquals(result.index(), 11);
        assertLogIndices(log, 11, 10, 6);
    }


    // 5.3, fig. 7
    // Index  01 02 03 04 05 06 07 08 09 10 11 12
    // Leader 01 01 01 04 04 05 05 06 06 06
    // Flwr A 01 01 01 04 04 05 05 06 06 06 07 07
    public void testRAFTPaperScenarioD() throws Exception {
        initB();
        RaftImpl impl=getImpl(b);
        Log log=impl.raft().log();
        append(impl,  1, 0, new LogEntry(1, buf), leader, 1);
        append(impl,  2, 1, new LogEntry(1, buf), leader, 1);
        append(impl,  3, 1, new LogEntry(1, buf), leader, 1);
        append(impl,  4, 1, new LogEntry(4, buf), leader, 1);
        append(impl,  5, 4, new LogEntry(4, buf), leader, 1);
        append(impl,  6, 4, new LogEntry(5, buf), leader, 1);
        append(impl,  7, 5, new LogEntry(5, buf), leader, 1);
        append(impl,  8, 5, new LogEntry(6, buf), leader, 1);
        append(impl,  9, 6, new LogEntry(6, buf), leader, 1);
        append(impl, 10, 6, new LogEntry(6, buf), leader, 1);
        append(impl, 11, 6, new LogEntry(7, buf), leader, 1);
        append(impl, 12, 7, new LogEntry(7, buf), leader, 10);

        // add 11
        AppendResult result=append(impl, buf, leader, 10, 6, 8, 10);
        assertTrue(result.success());
        assertEquals(result.index(), 11);
        assertLogIndices(log, 11, 10, 8);

        // add 12
        result=append(impl, buf, leader, 11, 8, 8, 10);
        assertTrue(result.success());
        assertEquals(result.index(), 12);
        assertLogIndices(log, 12, 10, 8);

        // commit 12
        result=append(impl, null, leader, 0, 0, 0, 12);
        assertTrue(result.success());
        assertEquals(result.index(), 12);
        assertLogIndices(log, 12, 12, 8);
    }

    // 5.3, fig. 7
    // Index  01 02 03 04 05 06 07 08 09 10 11 12
    // Leader 01 01 01 04 04 05 05 06 06 06
    // Flwr A 01 01 01 04 04 04 04
    public void testRAFTPaperScenarioE() throws Exception {
        initB();
        RaftImpl impl=getImpl(b);
        Log log=impl.raft().log();
        append(impl,  1, 0, new LogEntry(1, buf), leader, 1);
        append(impl,  2, 1, new LogEntry(1, buf), leader, 1);
        append(impl,  3, 1, new LogEntry(1, buf), leader, 1);
        append(impl,  4, 1, new LogEntry(4, buf), leader, 1);
        append(impl,  5, 4, new LogEntry(4, buf), leader, 1);
        append(impl,  6, 4, new LogEntry(4, buf), leader, 1);
        append(impl,  7, 4, new LogEntry(4, buf), leader, 3);

        System.out.printf("log entries of follower before fix:\n%s", printLog(log));

        AppendResult result=append(impl, 11, 6, new LogEntry(6, buf), leader, 10);
        assertFalse(result.success());
        assertEquals(result.index(), 7);
        assertLogIndices(log, 7, 3, 4);

        // now try to append 8 (fails because of wrong term)
        result=append(impl, 8, 5, new LogEntry(6, buf), leader, 10);
        assert !result.success();
        assertEquals(result.index(), 4);
        assert result.commitIndex() == 3;
        assertLogIndices(log, 3, 3, 1);

        // now append 4-10
        for(int i=4; i <= 10; i++) {
            result=append(impl, i, terms[i-1], new LogEntry(terms[i], buf), leader, 10);
            assert result.success();
            assertEquals(result.index(), i);
            assert result.commitIndex() == i;
            assertLogIndices(log, i, i, terms[i]);
        }
        System.out.printf("log entries of follower after fix:\n%s", printLog(log));
        for(int i=0; i < terms.length; i++) {
            LogEntry entry=log.get(i);
            assert entry == null && i == 0 || entry.term() == terms[i];
        }
    }


    // 5.3, fig. 7
    // Index  01 02 03 04 05 06 07 08 09 10 11 12
    // Leader 01 01 01 04 04 05 05 06 06 06
    // Flwr A 01 01 01 02 02 02 03 03 03 03 03
    public void testRAFTPaperScenarioF() throws Exception {
        initB();
        RaftImpl impl=getImpl(b);
        Log log=impl.raft().log();
        int[] incorrect_terms={0,1,1,1,2,2,2,3,3,3,3,3};
        for(int i=1; i < incorrect_terms.length; i++)
            append(impl, i, incorrect_terms[i-1], new LogEntry(incorrect_terms[i], buf), leader, 3);

        System.out.printf("log entries of follower before fix:\n%s", printLog(log));

        AppendResult result=append(impl, 10, 6, new LogEntry(6, buf), leader, 10);
        assert !result.success();
        assert result.index() == 7;
        assertLogIndices(log, 6, 3, 2);

        System.out.printf("log entries of follower after first fix:\n%s", printLog(log));

        result=append(impl, 7, 5, new LogEntry(5, buf), leader, 10);
        assert !result.success();
        assert result.index() == 4;
        assertLogIndices(log, 3, 3, 1);

        System.out.printf("log entries of follower after second fix:\n%s", printLog(log));

        // now append 4-10:
        for(int i=4; i <= 10; i++) {
            result=append(impl, i, terms[i-1], new LogEntry(terms[i], buf), leader, 10);
            assert result.success();
            assertEquals(result.index(), i);
            assert result.commitIndex() == i;
            assertLogIndices(log, i, i, terms[i]);
        }
        System.out.printf("log entries of follower after final fix:\n%s", printLog(log));
        for(int i=0; i < terms.length; i++) {
            LogEntry entry=log.get(i);
            assert entry == null && i == 0 || entry.term() == terms[i];
        }
    }


    protected JChannel create(String name) throws Exception {
        return create(name, r -> r);
    }

    protected JChannel create(String name, Function<RAFT, RAFT> config) throws Exception {
        return create(name, members, config);
    }

    protected static JChannel create(String name, final List<String> members) throws Exception {
        return create(name, members, r -> r);
    }

    protected static JChannel create(String name, final List<String> members, Function<RAFT, RAFT> config) throws Exception {
        ELECTION election=new ELECTION();
        RAFT raft=config.apply(new RAFT()).members(members).raftId(name)
          .logClass("org.jgroups.protocols.raft.InMemoryLog").logPrefix(name + "-" + CLUSTER);
        return new JChannel(Util.getTestStack(election, raft, new REDIRECT())).name(name);
    }


    protected static void close(JChannel... channels) {
        for(JChannel ch: channels) {
            if(ch == null)
                continue;
            RAFT raft=ch.getProtocolStack().findProtocol(RAFT.class);
            try {
                Utils.deleteLog(raft);
            }
            catch(Exception ignored) {}
            Util.close(ch);
        }
    }

    protected void init(boolean verbose) throws Exception {
        init(verbose, r -> r);
    }

    protected void init(boolean verbose, Function<RAFT,RAFT> config) throws Exception {
        a=create("A", config); // leader
        as=new ReplicatedStateMachine<>(a);
        a.connect(CLUSTER);

        b=create("B", config);  // follower
        bs=new ReplicatedStateMachine<>(b);
        b.connect(CLUSTER);

        c=create("C", config);  // follower
        cs=new ReplicatedStateMachine<>(c);
        c.connect(CLUSTER);
        Util.waitUntilAllChannelsHaveSameView(10000, 500, a,b,c);
        Util.waitUntil(5000, 100,
                       () -> Stream.of(a,b,c).map(AppendEntriesTest::raft).allMatch(r -> r.leader() != null),
                       () -> Stream.of(a,b,c).map(ch -> String.format("%s: leader=%s", ch.getAddress(), raft(ch).leader()))
                         .collect(Collectors.joining("\n")));

        for(int i=0; i < 20; i++) {
            if(isLeader(a) && !isLeader(b) && !isLeader(c))
                break;
            Util.sleep(500);
        }
        if(verbose) {
            System.out.println("A: is leader? -> " + isLeader(a));
            System.out.println("B: is leader? -> " + isLeader(b));
            System.out.println("C: is leader? -> " + isLeader(c));
        }
        assert isLeader(a);
        assert !isLeader(b);
        assert !isLeader(c);
    }

    protected void initB() throws Exception {
        b=create("B"); // follower
        raft(b).stateMachine(new DummyStateMachine());
        b.connect(CLUSTER);
    }

    protected static boolean isLeader(JChannel ch) {
        RAFT raft=raft(ch);
        return raft.leader() != null && ch.getAddress().equals(raft.leader());
    }

    protected static RaftImpl getImpl(JChannel ch) {
        RAFT raft=ch.getProtocolStack().findProtocol(RAFT.class);
        return raft.impl();
    }

    protected static String printLog(Log l) {
        StringBuilder sb=new StringBuilder();
        l.forEach((e,i) -> sb.append(String.format("%d: term=%d\n", i, e.term())));
        return sb.toString();
    }

    protected static void assertLeader(JChannel ch, long timeout, long interval) {
        RAFT raft=raft(ch);
        long stop_time=System.currentTimeMillis() + timeout;
        while(System.currentTimeMillis() < stop_time) {
            if(raft.isLeader())
                break;
            Util.sleep(interval);
        }
        assert raft.isLeader();
    }

    protected void assertPresent(int key, int value, ReplicatedStateMachine<Integer,Integer> ... rsms) {
        if(rsms == null || rsms.length == 0)
            rsms=new ReplicatedStateMachine[]{as,bs,cs};
        for(int i=0; i < 10; i++) {
            boolean found=true;
            for(ReplicatedStateMachine<Integer,Integer> rsm: rsms) {
                Integer val=rsm.get(key);
                if(!Objects.equals(val, value)) {
                    found=false;
                    break;
                }
            }
            if(found)
                break;
            Util.sleep(500);
        }

        for(ReplicatedStateMachine<Integer,Integer> rsm: rsms) {
            Integer val=rsm.get(key);
            assert Objects.equals(val, value);
            System.out.println("rsm = " + rsm);
        }
    }

    @SafeVarargs
    protected final void assertSame(ReplicatedStateMachine<Integer,Integer>... rsms) {
        assertSame(TIMEOUT, INTERVAL, rsms);
    }


    @SafeVarargs
    protected final void assertSame(long timeout, long interval, ReplicatedStateMachine<Integer,Integer>... rsms) {
        Util.waitUntilTrue(timeout, interval, () -> Stream.of(rsms).allMatch(r -> r.equals(rsms[0])));
        for(ReplicatedStateMachine<Integer,Integer> rsm: rsms)
            System.out.println(rsm.channel().getName() + ": " + rsm);
        for(int i=1; i < rsms.length; i++) {
            assert rsms[i].equals(rsms[0])
              : String.format("commit-table of A: %s", ((RAFT)a.getProtocolStack().findProtocol(RAFT.class)).dumpCommitTable());
        }
    }

    protected static void assertLogIndices(Log log, int last_appended, int commit_index, int term) {
        assertEquals(log.lastAppended(), last_appended);
        assertEquals(log.commitIndex(), commit_index);
        assertEquals(log.currentTerm(), term);
    }

    protected static void assertCommitIndex(long timeout, long interval, long expected_commit, long expected_applied,
                                            JChannel... channels) {
        long target_time=System.currentTimeMillis() + timeout;
        while(System.currentTimeMillis() <= target_time) {
            boolean all_ok=true;
            for(JChannel ch: channels) {
                RAFT raft=raft(ch);
                if(expected_commit != raft.commitIndex() || expected_applied != raft.lastAppended())
                    all_ok=false;
            }
            if(all_ok)
                break;
            Util.sleep(interval);
        }
        for(JChannel ch: channels) {
            RAFT raft=raft(ch);
            System.out.printf("%s: last-applied=%d, commit-index=%d\n", ch.getAddress(), raft.lastAppended(), raft.commitIndex());
            assert raft.commitIndex() == expected_commit && raft.lastAppended() == expected_applied
              : String.format("%s: last-applied=%d, commit-index=%d", ch.getAddress(), raft.lastAppended(), raft.commitIndex());
        }
    }

    protected static RAFT raft(JChannel ch) {
        return ch.getProtocolStack().findProtocol(RAFT.class);
    }

    protected static AppendResult append(RaftImpl impl, long index, long prev_term, LogEntry entry, Address leader,
                                         long leader_commit) throws Exception {
        return append(impl, entry.command(), leader, Math.max(0, index-1), prev_term, entry.term(), leader_commit);
    }

    protected static AppendResult append(RaftImpl impl, byte[] data, Address leader,
                                         long prev_log_index, long prev_log_term, long entry_term, long leader_commit) throws Exception {
        int len=data != null? data.length : 0;
        LogEntries entries=new LogEntries().add(new LogEntry(entry_term, data, 0, len));
        return impl.handleAppendEntriesRequest(entries, leader, prev_log_index, prev_log_term, entry_term, leader_commit);
    }

}
