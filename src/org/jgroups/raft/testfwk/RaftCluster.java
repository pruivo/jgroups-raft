package org.jgroups.raft.testfwk;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.View;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import static org.jgroups.Message.TransientFlag.DONT_LOOPBACK;

/**
 * Orchestrates a number of {@link RaftNode} objects, to be used in a unit test, for example a leader and a follower.
 * @author Bela Ban
 * @since  1.0.5
 */
public class RaftCluster {
    // used to 'send' requests between the various instances
    protected final Map<Address,RaftNode> nodes=new ConcurrentHashMap<>();

    public RaftCluster add(Address addr, RaftNode node) {
        nodes.put(addr, node);
        return this;
    }

    public RaftCluster remove(Address addr) {nodes.remove(addr); return this;}
    public RaftCluster clear()              {nodes.clear(); return this;}

    public void handleView(View view) {
        List<Address> members=view.getMembers();
        nodes.keySet().retainAll(Objects.requireNonNull(members));
        nodes.values().forEach(n -> n.raft().handleView(view));
    }

    public void send(Message msg) {
        Address dest=msg.dest(), src=msg.src();
        if(dest != null) {
            RaftNode node=nodes.get(dest);
            node.up(msg);
        }
        else {
            for(Map.Entry<Address,RaftNode> e: nodes.entrySet()) {
                Address d=e.getKey();
                RaftNode n=e.getValue();
                if(Objects.equals(d, src) && msg.isFlagSet(DONT_LOOPBACK))
                    continue;
                n.up(msg);
            }
        }
    }

    public String toString() {
        return String.format("%d nodes: %s", nodes.size(), nodes.keySet());
    }
}
