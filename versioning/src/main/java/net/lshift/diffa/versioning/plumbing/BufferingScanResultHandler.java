package net.lshift.diffa.versioning.plumbing;

import net.lshift.diffa.adapter.scanning.ScanResultEntry;
import net.lshift.diffa.scanning.ScanResultHandler;

import java.util.HashSet;
import java.util.Set;

public class BufferingScanResultHandler implements ScanResultHandler {

  private Set<ScanResultEntry> entries = new HashSet<ScanResultEntry>();

  @Override
  public void onEntry(ScanResultEntry entry) {
    entries.add(entry);
  }

  public Set<ScanResultEntry> getEntries() {
    return entries;
  }
}
