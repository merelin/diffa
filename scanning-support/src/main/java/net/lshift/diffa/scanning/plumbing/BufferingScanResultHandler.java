package net.lshift.diffa.scanning.plumbing;

import net.lshift.diffa.adapter.scanning.ScanResultEntry;
import net.lshift.diffa.scanning.ScanResultHandler;

import java.util.*;

/**
 * By default this sorts by insertion order of the scan result, which is generally the DB order.
 *
 * If you need a different sort algorithm, you need to provide a suitable comparator.
 */
public class BufferingScanResultHandler implements ScanResultHandler {

  private final Set<ScanResultEntry> entries;

  public BufferingScanResultHandler() {

    this.entries = new LinkedHashSet<ScanResultEntry>();
  }

  public BufferingScanResultHandler(Comparator<ScanResultEntry> comparator) {
    this.entries = new TreeSet<ScanResultEntry>(comparator);
  }

  @Override
  public void onEntry(ScanResultEntry entry) {
    entries.add(entry);
  }

  @Override
  public void onCompletion() {}

  public Set<ScanResultEntry> getEntries() {
    return entries;
  }
}
