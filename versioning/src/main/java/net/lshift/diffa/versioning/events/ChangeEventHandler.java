package net.lshift.diffa.versioning.events;

public interface ChangeEventHandler {
  void onEvent(Long endpoint, ChangeEvent event);
}
