package org.jgroups.raft2;

import java.util.concurrent.ThreadLocalRandom;

import org.jgroups.protocols.raft2.RaftConfigurationException;
import org.jgroups.util.TimeScheduler;

/**
 * TODO! document this
 *
 * @author Pedro Ruivo
 * @since 12
 */
public interface ElectionTimeoutTask extends Runnable {

   static void validateTimeout(long minTimeoutMillis, long maxTimeoutMillis) throws RaftConfigurationException {
      checkPositive(minTimeoutMillis, "minTimeoutMillis");
      checkPositive(maxTimeoutMillis, "maxTimeoutMillis");
      if (minTimeoutMillis >= maxTimeoutMillis) {
         throw new RaftConfigurationException("minTimeoutMillis (" + minTimeoutMillis + ") must be lower than maxTimeoutMillis (" + maxTimeoutMillis + ")");
      }
   }

   static void checkPositive(long value, String field) throws RaftConfigurationException {
      if (value <= 0) {
         throw new RaftConfigurationException(field + " (" + value + ") must be greater than zero");
      }
   }

   static long computeElectionTimeout(long min, long max) throws RaftConfigurationException {
      validateTimeout(min, max);
      long diff = max - min;
      return (int) ((ThreadLocalRandom.current().nextDouble() * 100000) % diff) + min;
   }

   void add(ElectionTimeoutListener listener);

   void startTimer(TimeScheduler timeScheduler);

   void stopTimer();

   void onMessageReceived();
}
