package org.jgroups.tests.utils;

import java.util.concurrent.Executor;

/**
 * TODO! document this
 *
 * @author Pedro Ruivo
 * @since 12
 */
public enum WithinThreadExecutor implements Executor {
   INSTANCE;

   @Override
   public void execute(Runnable command) {
      try {
         command.run();
      } catch (Throwable e) {
         //ignored
      }
   }
}
