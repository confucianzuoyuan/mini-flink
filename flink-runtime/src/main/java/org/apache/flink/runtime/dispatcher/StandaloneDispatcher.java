package org.apache.flink.runtime.dispatcher;

import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobMaster;
import org.apache.flink.runtime.rpc.RpcService;

/**
 * Dispatcher implementation which spawns a {@link JobMaster} for each
 * submitted {@link JobGraph} within in the same process. This dispatcher
 * can be used as the default for all different session clusters.
 */
public class StandaloneDispatcher extends Dispatcher {
	public StandaloneDispatcher(
			RpcService rpcService,
			DispatcherId fencingToken,
			DispatcherBootstrap dispatcherBootstrap,
			DispatcherServices dispatcherServices) throws Exception {
		super(
			rpcService,
			fencingToken,
			dispatcherBootstrap,
			dispatcherServices);
	}
}
