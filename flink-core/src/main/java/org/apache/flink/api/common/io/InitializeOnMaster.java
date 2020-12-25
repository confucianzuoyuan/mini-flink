package org.apache.flink.api.common.io;

import org.apache.flink.annotation.Public;

import java.io.IOException;

@Public
public interface InitializeOnMaster {
	void initializeGlobal(int parallelism) throws IOException;
}
