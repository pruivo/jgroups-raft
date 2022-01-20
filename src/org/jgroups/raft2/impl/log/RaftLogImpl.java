package org.jgroups.raft2.impl.log;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.StampedLock;

import org.jgroups.raft.util.Bits2;
import org.jgroups.raft2.LogEntry;
import org.jgroups.raft2.impl.AbstractEventLoop;
import org.jgroups.raft2.impl.AppendRequestHeader;
import org.jgroups.raft2.impl.RaftId;
import org.jgroups.raft2.impl.RaftRole;
import org.jgroups.raft2.impl.eventloop.RaftRoleEventLoop;
import org.jgroups.raft2.impl.state.RaftState;
import org.jgroups.util.Util;

/**
 * TODO! document this
 *
 * @author Pedro Ruivo
 * @since 12
 */
public class RaftLogImpl extends AbstractEventLoop implements RaftLog {

   private final StampedLock resizeLock = new StampedLock();
   private final Object appendLock = new Object();

   private final RaftRoleEventLoop roleEventLoop;
   private final RaftState state;
   private final Collection<RaftLogListener> listeners = new CopyOnWriteArrayList<>();

   private volatile LogEntry[] entries = new LogEntry[256];
   private volatile int lastEntryIdx = -1;
   private volatile int commitIndex;


   public RaftLogImpl(Executor executor, RaftRoleEventLoop roleEventLoop, RaftState state) {
      super(executor);
      this.roleEventLoop = roleEventLoop;
      this.state = state;
   }

   @Override
   public void addListener(RaftLogListener listener) {
      listeners.add(Objects.requireNonNull(listener));
   }

   @Override
   public LogEntry append(byte[] data, int offset, int length) {
      long stateStamp = state.getStateLock().readLock();
      try {
         if (state.getRole() != RaftRole.LEADER) {
            // not a leader
            return null;
         }
         long stamp = resizeLock.readLock();
         try {
            synchronized (appendLock) {
               int appendIndex;
               int logEntryIndex;
               stamp = resizeIfRequired(stamp, 1);
               if (lastEntryIdx == -1) {
                  // first entry
                  appendIndex = 0;
                  logEntryIndex = 1;
               } else {
                  LogEntry entry = entries[lastEntryIdx];
                  assert entry != null;
                  appendIndex = lastEntryIdx + 1;
                  logEntryIndex = entry.getLogIndex() + 1;
               }

               LogEntry entry = new DataLogEntry(state.getTerm(), logEntryIndex, data, offset, length);
               entries[appendIndex] = entry;
               lastEntryIdx = appendIndex;
               return entry;
            }
         } finally {
            resizeLock.unlock(stamp);
         }
      } finally {
         state.getStateLock().unlockRead(stateStamp);
      }
   }

   @Override
   public void appendFromLeader(AppendRequestHeader header, byte[] data, int offset, int length) throws IOException, ClassNotFoundException {
      TermResult termResult = checkTerm(header.getTerm(), header.getLeader());
      try {
         if (!termResult.accepted) {
            //TODO rejected, old term!
            return;
         }
         if (state.getRole() == RaftRole.LEADER) {
            // old message, we are the leader now. we can ignore it
            return;
         }

         // check the data
         DataInput dataInput = new DataInputStream(new ByteArrayInputStream(data, offset, length));
         int numberOfEntries = Bits2.readIntCompressed(dataInput);
         if (numberOfEntries == 0) {
            // heart beat, already handled
            return;
         }

         // read lock to prevent resize/truncate
         long stamp = resizeLock.readLock();
         try {
            LogEntry prevEntry = findEntryNoLock(header.getPrevLogIdx());
            if (prevEntry == null) {
               //TODO rejected
               return;
            }
            if (!prevEntry.isSame(header.getPrevLogIdx(), header.getPrevLogIdx())) {
               //TODO rejected
               return;
            }

            // append accepted
            // append lock not required...
            synchronized (appendLock) {
               stamp = resizeIfRequired(stamp, numberOfEntries);
               int prevIndex = prevEntry.getLogIndex() == 0 ? -1 : prevEntry.getLogIndex() - entries[0].getLogIndex();
               for (int i = 0; i < numberOfEntries; ++i) {
                  LogEntry newEntry = Util.readGenericStreamable(dataInput);
                  assert prevIndex == -1 || entries[prevIndex].getLogIndex() == newEntry.getLogIndex() - 1;
                  ++prevIndex;
                  entries[prevIndex] = newEntry;
               }
               lastEntryIdx = prevIndex;
               // TODO accepted
            }
         } finally {
            resizeLock.unlock(stamp);
         }
      } finally {
         state.getStateLock().unlock(termResult.stamp);
      }
   }

   @Override
   public LogEntry getLast() {
      long stamp = resizeLock.readLock();
      try {
         if (lastEntryIdx < 0) {
            return InitialLogEntry.INSTANCE;
         }
         return entries[lastEntryIdx];
      } finally {
         resizeLock.unlockRead(stamp);
      }
   }


   private TermResult checkTerm(int newTerm, RaftId leader) {
      long stamp = state.getStateLock().readLock();
      if (newTerm < state.getTerm()) {
         // old term
         return new TermResult(stamp, false);
      }
      if (newTerm == state.getTerm()) {
         // same term
         return new TermResult(stamp, true);
      }
      long ws = state.getStateLock().tryConvertToWriteLock(stamp);
      if (ws == 0) {
         // failed to convert
         state.getStateLock().unlockRead(stamp);
         ws = state.getStateLock().writeLock();
         // need to double-check
         if (newTerm < state.getTerm()) {
            return new TermResult(ws, false);
         }
         if (newTerm == state.getTerm()) {
            stamp = state.getStateLock().tryConvertToReadLock(ws);
            return new TermResult(stamp, true);
         }
      }

      // at this point, write lock is acquired
      state.onNewTerm(newTerm, leader);
      stamp = state.getStateLock().tryConvertToReadLock(ws);
      return new TermResult(stamp, true);
   }

   private long resizeIfRequired(long readStamp, int size) {
      if (lastEntryIdx + size < entries.length) {
         return readStamp;
      }
      long ws = resizeLock.tryConvertToWriteLock(readStamp);
      if (ws == 0) {
         resizeLock.unlock(readStamp);
         ws = resizeLock.writeLock();
         if (lastEntryIdx + size < entries.length) {
            return resizeLock.tryConvertToReadLock(ws);
         }
      }
      //noinspection NonAtomicOperationOnVolatileField
      entries = Arrays.copyOf(entries, entries.length + (size * 2));
      return resizeLock.tryConvertToReadLock(ws);
   }

   private LogEntry findEntryNoLock(int logIndex) {
      LogEntry first = entries[0];
      if (first == null) {
         return InitialLogEntry.INSTANCE;
      }
      if (first.getLogIndex() > logIndex) {
         // index in the past, already committed
         return null;
      }
      int entryIdx = logIndex - first.getLogIndex();
      if (entryIdx > entries.length) {
         // index in the future
         return null;
      }
      assert entryIdx >= 0 && entryIdx < entries.length;
      return entryIdx > lastEntryIdx ? null : entries[entryIdx];
   }

   @Override
   protected void onEvent() {

   }


   private static class TermResult {
      final long stamp;
      final boolean accepted;

      TermResult(long stamp, boolean accepted) {
         this.stamp = stamp;
         this.accepted = accepted;
      }
   }
}
