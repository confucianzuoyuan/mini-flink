package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.jobmaster.SlotOwner;

import javax.annotation.Nonnull;

public interface Scheduler extends SlotProvider, SlotOwner {
	void start(@Nonnull ComponentMainThreadExecutor mainThreadExecutor);
}
