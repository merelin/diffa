package net.lshift.diffa.scanning;

import net.lshift.diffa.adapter.scanning.ScanResultEntry;

public interface ScanResultHandler {

  void onEntry(ScanResultEntry entry);

  void onCompletion();
}
