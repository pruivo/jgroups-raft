package org.jgroups.raft2;

import java.util.Collection;

import org.jgroups.protocols.raft2.RAFT2;
import org.jgroups.raft2.impl.AppendRequestHeader;
import org.jgroups.raft2.impl.AppendResponseHeader;
import org.jgroups.raft2.impl.election.VoteRequestHeader;
import org.jgroups.raft2.impl.election.VoteResponseHeader;

/**
 * TODO! document this
 *
 * @author Pedro Ruivo
 * @since 12
 */
public interface RaftEventHandler {

   void init(RAFT2 protocol, Collection<String> initialMembers);

   void start();

   void stop();

   void disconnect();

   void onAppendRequest(AppendRequestHeader header, byte[] data, int offset, int length);

   void onAppendResponse(AppendResponseHeader header);

   void onVoteRequest(VoteRequestHeader header);

   void onVoteResponse(VoteResponseHeader voteResponseHeader);

}
