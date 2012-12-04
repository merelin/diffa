package net.lshift.diffa.sql;

import net.lshift.diffa.adapter.scanning.ScanResultEntry;

public interface ScanResultHandler {

  void onEntry(ScanResultEntry entry);

}
