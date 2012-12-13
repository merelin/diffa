package net.lshift.diffa.conductor.plumbing;

import net.lshift.diffa.adapter.scanning.ScanResultEntry;
import net.lshift.diffa.railyard.Answer;

public class ScanResultAnswer implements Answer {

  private ScanResultEntry entry;

  public ScanResultAnswer(ScanResultEntry entry) {
    this.entry = entry;
  }
}
