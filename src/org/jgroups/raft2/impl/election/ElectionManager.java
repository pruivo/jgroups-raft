package org.jgroups.raft2.impl.election;

import org.jgroups.raft2.ElectionTimeoutListener;

/**
 * TODO! document this
 *
 * @author Pedro Ruivo
 * @since 12
 */
public interface ElectionManager extends ElectionTimeoutListener {

   void onVoteRequest(VoteRequestHeader header);

   void onVoteResponse(VoteResponseHeader header);

}
