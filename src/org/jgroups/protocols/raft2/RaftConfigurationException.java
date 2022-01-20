package org.jgroups.protocols.raft2;

/**
 * TODO! document this
 *
 * @author Pedro Ruivo
 * @since 12
 */
public class RaftConfigurationException extends Exception {

   public RaftConfigurationException() {
   }

   public RaftConfigurationException(String message) {
      super(message);
   }

   public RaftConfigurationException(String message, Throwable cause) {
      super(message, cause);
   }

   public RaftConfigurationException(Throwable cause) {
      super(cause);
   }
}
