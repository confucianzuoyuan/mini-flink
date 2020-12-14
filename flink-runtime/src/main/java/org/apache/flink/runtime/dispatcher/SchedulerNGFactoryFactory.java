package org.apache.flink.runtime.dispatcher;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.scheduler.DefaultSchedulerFactory;
import org.apache.flink.runtime.scheduler.SchedulerNGFactory;

/**
 * Factory for {@link SchedulerNGFactory}.
 */
public final class SchedulerNGFactoryFactory {

	public static final String SCHEDULER_TYPE_NG = "ng";

	private SchedulerNGFactoryFactory() {}

	public static SchedulerNGFactory createSchedulerNGFactory(final Configuration configuration) {
		final String schedulerName = configuration.getString(JobManagerOptions.SCHEDULER);
		switch (schedulerName) {
			case SCHEDULER_TYPE_NG:
				return new DefaultSchedulerFactory();

			default:
				throw new IllegalArgumentException(String.format(
					"Illegal value [%s] for config option [%s]",
					schedulerName,
					JobManagerOptions.SCHEDULER.key()));
		}
	}
}
