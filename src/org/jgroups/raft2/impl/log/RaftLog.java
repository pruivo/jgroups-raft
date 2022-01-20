package org.jgroups.raft2.impl.log;

import java.io.IOException;

import org.jgroups.raft2.LogEntry;
import org.jgroups.raft2.impl.AppendRequestHeader;
import org.jgroups.raft2.impl.state.RaftStateListener;

/**
 * TODO! document this
 *
 * @author Pedro Ruivo
 * @since 12
 */
public interface RaftLog {

   void addListener(RaftLogListener listener);

   LogEntry append(byte[] data, int offset, int length);

   void appendFromLeader(AppendRequestHeader header, byte[] data, int offset, int length) throws IOException, ClassNotFoundException;

   LogEntry getLast();

}
