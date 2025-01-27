package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.protocols.raft.ELECTION;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.protocols.raft.REDIRECT;
import org.jgroups.raft.util.Utils;
import org.jgroups.util.CompletableFutures;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Tests the addServer() / removeServer) functionality
 * @author Bela Ban
 * @since  0.2
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class DynamicMembershipTest {
    protected JChannel[]           channels;
    protected RAFT[]               rafts;
    protected Address              leader;
    protected List<String>         mbrs;
    protected static final String  CLUSTER=DynamicMembershipTest.class.getSimpleName();

    @AfterMethod protected void destroy() throws Exception {
        close(channels);
    }


    /** Start a member not in {A,B,C} -> expects an exception */
    public void testStartOfNonMember() {
        JChannel non_member=null;
        try {
            init("A", "B", "C");
            channels=Arrays.copyOf(channels, channels.length+1);
            channels[channels.length-1]=create("X");
            assert false : "Starting a non-member should throw an exception";
        }
        catch(Exception e) {
            System.out.println("received exception (as expected): " + e);
        }
        finally {
            close(non_member);
        }
    }

    /** Calls addServer() on non-leader. Calling {@link org.jgroups.protocols.raft.RAFT#addServer(String)}
     * must throw an exception */
    public void testMembershipChangeOnNonLeader() throws Exception {
        init("A","B");
        RAFT raft=raft(channels[1]);  // non-leader B
        try {
            raft.addServer("X");
            assert false : "Calling RAFT.addServer() on a non-leader must throw an exception";
        }
        catch(Exception ex) {
            System.out.println("received exception calling RAFT.addServer() on a non-leader (as expected): " + ex);
        }
    }

    /** {A,B,C} +D +E -E -D. Note that we can _add_ a 6th server, as adding it requires the majority of the existing
     * servers (3 of 5). However, _removing_ the 6th server won't work, as we require the majority (4 of 6) to remove
     * the 6th server. This is because we only have 3 'real' channels (members).
     */
    public void testSimpleAddAndRemove() throws Exception {
        init("A", "B", "C");
        leader=leader(10000, 500, channels);
        System.out.println("leader = " + leader);
        assert leader != null;
        waitUntilAllRaftsHaveLeader(channels);
        assertSameLeader(leader, channels);
        assertMembers(5000, 200, mbrs, 2, channels);

        List<String> new_mbrs=List.of("D", "E");
        RAFT raft=raft(leader);
        List<String> expected_mbrs=new ArrayList<>(mbrs);

        // adding:
        for(String mbr: new_mbrs) {
            System.out.printf("\nAdding %s\n", mbr);
            raft.addServer(mbr);
            expected_mbrs.add(mbr);
            assertMembers(10000, 200, expected_mbrs, expected_mbrs.size()/2+1, channels);
        }

        // removing:
        for(int i=new_mbrs.size()-1; i >= 0; i--) {
            String mbr=new_mbrs.get(i);
            System.out.printf("\nRemoving %s\n", mbr);
            raft.removeServer(mbr);
            expected_mbrs.remove(mbr);
            assertMembers(10000, 200, expected_mbrs, expected_mbrs.size()/2+1, channels);
        }
    }

    /**
     * {A,B,C} +D +E +F +G +H +I +J
     */
    public void testAddServerSimultaneously() throws Exception {
        init("A", "B", "C", "D");
        leader = leader(10000, 500, channels);
        System.out.println("leader = " + leader);
        assert leader != null;
        waitUntilAllRaftsHaveLeader(channels);
        assertSameLeader(leader, channels);
        assertMembers(5000, 500, mbrs, mbrs.size()/2+1, channels);

        final RAFT raft = raft(leader);

        final List<String> newServers = Arrays.asList("E", "F", "G");
        final CountDownLatch addServerLatch = new CountDownLatch(1);

        for (String newServer: newServers) {
            new Thread(() -> {
                try {
                    addServerLatch.await();
                    CompletableFuture<byte[]> f=raft.addServer(newServer);
                    CompletableFutures.join(f);
                    System.out.printf("[%d]: added %s successfully\n", Thread.currentThread().getId(), newServer);
                }
                catch (Throwable t) {
                    t.printStackTrace();
                }
            }).start();
        }
        addServerLatch.countDown();

        List<String> expected_mbrs=new ArrayList<>(this.mbrs);
        expected_mbrs.addAll(newServers);
        assertMembers(20000, 500, expected_mbrs, expected_mbrs.size()/2+1, channels);
        System.out.printf("\nmembers:\n%s\n", Stream.of(rafts).map(r -> String.format("%s: %s", r.getAddress(), r.members()))
          .collect(Collectors.joining("\n")));
    }

    /**
     * {A,B} -> -B -> {A} (A is the leader). Call addServer("C"): this will fail as A is not the leader anymore. Then
     * make B rejoin, and addServer("C") will succeed
     */
    public void testAddServerOnLeaderWhichCantCommit() throws Exception {
        init("A", "B");
        Util.waitUntilAllChannelsHaveSameView(10000, 500, channels);
        leader=leader(10000, 500, channels);
        System.out.println("leader = " + leader);
        assert leader != null;

        // close non-leaders
        for(JChannel ch: channels)
            if(!ch.getAddress().equals(leader))
                close(ch);

        RAFT raft=raft(leader);
        try { // this will fail as leader A stepped down when it found that the view's size dropped below the majority
            raft.addServer("C");
            assert false : "Adding server C should fail as the leader stepped down";
        }
        catch(Exception ex) {
            System.out.println("Caught exception (as expected) trying to add C: " + ex);
        }

        // Now start B again, so that addServer("C") can succeed
        for(int i=0; i < channels.length; i++) {
            if(channels[i].isClosed())
                channels[i]=create(String.valueOf((char)('A' + i)));
        }
        Util.waitUntilAllChannelsHaveSameView(10000, 500, channels);

        leader=leader(10000, 500, channels);
        System.out.println("leader = " + leader);
        assert leader != null;
        raft=raft(leader);
        raft.addServer("C"); // adding C should now succeed, as we have a valid leader again

        // Now create and connect C
        channels=Arrays.copyOf(channels, 3);
        if(!this.mbrs.contains("C"))
            this.mbrs.add("C");
        channels[2]=create("C");

        assertMembers(10000, 500, mbrs, 2, channels);

        // wait until everyone has committed the addServer(C) operation
        assertCommitIndex(20000, 500, raft(leader).lastAppended(), channels);
    }

    protected void init(String ... nodes) throws Exception {
        mbrs=new ArrayList<>(List.of(nodes));
        channels=new JChannel[nodes.length];
        rafts=new RAFT[nodes.length];
        for(int i=0; i < nodes.length; i++) {
            channels[i]=create(nodes, i);
            rafts[i]=raft(channels[i]);
        }
    }

    protected JChannel create(String name) throws Exception {
        RAFT raft=new RAFT().members(mbrs).raftId(name).stateMachine(new DummyStateMachine())
          .logClass("org.jgroups.protocols.raft.InMemoryLog").logPrefix(name + "-" + CLUSTER);
        JChannel ch=new JChannel(Util.getTestStack(new ELECTION(), raft, new REDIRECT())).name(name);
        ch.connect(CLUSTER);
        return ch;
    }

    protected static JChannel create(String[] names, int index) throws Exception {
        RAFT raft=new RAFT().members(Arrays.asList(names)).raftId(names[index]).stateMachine(new DummyStateMachine())
          .logClass("org.jgroups.protocols.raft.InMemoryLog").logPrefix(names[index] + "-" + CLUSTER)
          .resendInterval(500);
        JChannel ch=new JChannel(Util.getTestStack(new ELECTION(), raft, new REDIRECT())).name(names[index]);
        ch.connect(CLUSTER);
        return ch;
    }

    protected static Address leader(long timeout, long interval, JChannel ... channels) {
        long target_time=System.currentTimeMillis() + timeout;
        while(System.currentTimeMillis() <= target_time) {
            for(JChannel ch: channels) {
                if(ch.isConnected() && raft(ch).isLeader())
                    return raft(ch).leader();
            }
            Util.sleep(interval);
        }
        return null;
    }

    protected static void assertSameLeader(Address leader, JChannel... channels) {
        for(JChannel ch: channels) {
            final Address raftLeader = raft(ch).leader();
            assert leader.equals(raftLeader)
                : String.format("expected leader to be '%s' but was '%s'", leader, raftLeader);
        }
    }

    protected static void assertMembers(long timeout, long interval, List<String> members, int expected_majority, JChannel... channels) {
        long target_time=System.currentTimeMillis() + timeout;
        while(System.currentTimeMillis() <= target_time) {
            boolean all_ok=true;
            for(JChannel ch: channels) {
                if(!ch.isConnected())
                    continue;
                RAFT raft=raft(ch);
                if(!new HashSet<>(raft.members()).equals(new HashSet<>(members)))
                    all_ok=false;
            }
            if(all_ok)
                break;
            Util.sleep(interval);
        }
        for(JChannel ch: channels) {
            if(!ch.isConnected())
                continue;
            RAFT raft=raft(ch);
            System.out.printf("%s: members=%s, majority=%d\n", ch.getAddress(), raft.members(), raft.majority());
            assert new HashSet<>(raft.members()).equals(new HashSet<>(members))
              : String.format("expected members=%s, actual members=%s", members, raft.members());

            assert raft.majority() == expected_majority
              : ch.getName() + ": expected majority=" + expected_majority + ", actual=" + raft.majority();
        }
    }

    protected static void assertCommitIndex(long timeout, long interval, long expected_commit, JChannel... channels) {
        long target_time=System.currentTimeMillis() + timeout;
        while(System.currentTimeMillis() <= target_time) {
            boolean all_ok=true;
            for(JChannel ch: channels) {
                RAFT raft=raft(ch);
                if(expected_commit != raft.commitIndex())
                    all_ok=false;
            }
            if(all_ok)
                break;
            Util.sleep(interval);
        }
        for(JChannel ch: channels) {
            RAFT raft=raft(ch);
            System.out.printf("%s: members=%s, last-applied=%d, commit-index=%d\n", ch.getAddress(), raft.members(),
                              raft.lastAppended(), raft.commitIndex());
            assert raft.commitIndex() == expected_commit : String.format("%s: last-applied=%d, commit-index=%d",
                                                                         ch.getAddress(), raft.lastAppended(), raft.commitIndex());
        }
    }

    protected static void waitUntilAllRaftsHaveLeader(JChannel[] channels) throws TimeoutException {
        Util.waitUntil(5000, 250, () -> Arrays.stream(channels).allMatch(ch -> raft(ch).leader() != null));
    }

    protected RAFT raft(Address addr) {
        return raft(channel(addr));
    }

    protected JChannel channel(Address addr) {
        for(JChannel ch: channels) {
            if(ch.getAddress() != null && ch.getAddress().equals(addr))
                return ch;
        }
        return null;
    }

    protected static RAFT raft(JChannel ch) {
        return ch.getProtocolStack().findProtocol(RAFT.class);
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

}
