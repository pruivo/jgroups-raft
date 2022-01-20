package org.jgroups.raft2.impl.election;

import java.util.Collection;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jgroups.protocols.raft2.RaftConfigurationException;
import org.jgroups.raft2.ElectionTimeoutListener;
import org.jgroups.raft2.ElectionTimeoutTask;
import org.jgroups.util.TimeScheduler;

import net.jcip.annotations.GuardedBy;

/**
 * TODO! document this
 *
 * @author Pedro Ruivo
 * @since 12
 */
public class ElectionTimeoutTaskImpl implements ElectionTimeoutTask {

   private final AtomicBoolean heartBeatReceived = new AtomicBoolean(false);
   private final Collection<ElectionTimeoutListener> listeners = new CopyOnWriteArrayList<>();
   private final long periodMillis;
   @GuardedBy("this")
   private Future<?> cancellationFuture;

   public static ElectionTimeoutTask create(long minTimeoutMillis, long maxTimeoutMillis) throws RaftConfigurationException {
      long timeout = ElectionTimeoutTask.computeElectionTimeout(minTimeoutMillis, maxTimeoutMillis);
      assert timeout > 0;
      return new ElectionTimeoutTaskImpl(timeout);
   }


   private ElectionTimeoutTaskImpl(long periodMillis) {
      this.periodMillis = periodMillis;
   }

   @Override
   public void add(ElectionTimeoutListener listener) {
      listeners.add(listener);
   }

   @Override
   public void startTimer(TimeScheduler timeScheduler) {
      if (timeScheduler == null) {
         return;
      }
      synchronized (this) {
         if (cancellationFuture != null) {
            cancellationFuture.cancel(false);
         }
         cancellationFuture = timeScheduler.scheduleAtFixedRate(this, periodMillis, periodMillis, TimeUnit.MILLISECONDS);
      }
   }

   @Override
   public void stopTimer() {
      synchronized (this) {
         if (cancellationFuture != null) {
            cancellationFuture.cancel(false);
         }
      }
   }

   @Override
   public void onMessageReceived() {
      heartBeatReceived.set(true);
   }

   @Override
   public void run() {
      if (heartBeatReceived.compareAndSet(true, false)) {
         // heartbeat received
         return;
      }
      for (ElectionTimeoutListener listener : listeners) {
         listener.onElectionTimeout();
      }
   }
}
