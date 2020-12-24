package org.apache.flink.runtime.state;

import javax.annotation.Nonnull;

public abstract class AbstractSnapshotStrategy<T extends StateObject> {

	@Nonnull
	protected final String description;

	protected AbstractSnapshotStrategy(@Nonnull String description) {
		this.description = description;
	}

	@Override
	public String toString() {
		return "SnapshotStrategy {" + description + "}";
	}
}
