package org.apache.flink.runtime.shuffle;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.InstantiationUtil;

import static org.apache.flink.runtime.shuffle.ShuffleServiceOptions.SHUFFLE_SERVICE_FACTORY_CLASS;

public enum ShuffleServiceLoader {
	;

	public static ShuffleServiceFactory<?, ?, ?> loadShuffleServiceFactory(Configuration configuration) throws FlinkException {
		String shuffleServiceClassName = configuration.getString(SHUFFLE_SERVICE_FACTORY_CLASS);
		ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		return InstantiationUtil.instantiate(
			shuffleServiceClassName,
			ShuffleServiceFactory.class,
			classLoader);
	}
}
