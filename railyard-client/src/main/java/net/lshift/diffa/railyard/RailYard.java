package net.lshift.diffa.railyard;

import net.lshift.diffa.events.ChangeEvent;

public interface RailYard {

  void postChanges(String space, String endpoint, Iterable<ChangeEvent> events);
}
