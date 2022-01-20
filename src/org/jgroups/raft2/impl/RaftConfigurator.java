package org.jgroups.raft2.impl;

import java.util.Arrays;

import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.raft2.RAFT2;
import org.jgroups.raft2.impl.election.VoteRequestHeader;
import org.jgroups.raft2.impl.election.VoteResponseHeader;
import org.jgroups.util.ExtendedUUID;
import org.jgroups.util.Util;

/**
 * TODO! document this
 *
 * @author Pedro Ruivo
 * @since 12
 */
public enum RaftConfigurator {
   ;

   private static final short PROTOCOL_ID = 525;
   public static final byte[] RAFT_KEY = Util.stringToBytes("raft-id");
   public static final short APPEND_REQ_ID = 2005;
   public static final short APPEND_RSP_ID = 2006;
   public static final short VOTE_REQ_ID = 2007;
   public static final short VOTE_RSP_ID = 2008;
   public static final short RAFT_ID_IMPL_ID = 2009;

   public static void configure() {
      ClassConfigurator.addProtocol(PROTOCOL_ID, RAFT2.class);
      ClassConfigurator.add(APPEND_REQ_ID, AppendRequestHeader.class);
      ClassConfigurator.add(APPEND_RSP_ID, AppendResponseHeader.class);
      ClassConfigurator.add(VOTE_REQ_ID, VoteRequestHeader.class);
      ClassConfigurator.add(VOTE_RSP_ID, VoteResponseHeader.class);
      ClassConfigurator.add(RAFT_ID_IMPL_ID, RaftIdImpl.class);
   }

   public static byte[] getRaftId(ExtendedUUID addr) {
      return addr.get(RAFT_KEY);
   }

   public static boolean isSameRaftId(byte[] raftId, ExtendedUUID addr) {
      return Arrays.equals(raftId, getRaftId(addr));
   }
}
