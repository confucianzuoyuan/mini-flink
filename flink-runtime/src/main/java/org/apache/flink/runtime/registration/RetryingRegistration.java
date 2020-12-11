package org.apache.flink.runtime.registration;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.rpc.FencedRpcGateway;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcService;
import org.slf4j.Logger;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkNotNull;

public abstract class RetryingRegistration<F extends Serializable, G extends RpcGateway, S extends RegistrationResponse.Success> {

	// ------------------------------------------------------------------------
	// Fields
	// ------------------------------------------------------------------------

	private final Logger log;

	private final RpcService rpcService;

	private final String targetName;

	private final Class<G> targetType;

	private final String targetAddress;

	private final F fencingToken;

	private final CompletableFuture<Tuple2<G, S>> completionFuture;

	private final RetryingRegistrationConfiguration retryingRegistrationConfiguration;

	private volatile boolean canceled;

	// ------------------------------------------------------------------------

	public RetryingRegistration(
			Logger log,
			RpcService rpcService,
			String targetName,
			Class<G> targetType,
			String targetAddress,
			F fencingToken,
			RetryingRegistrationConfiguration retryingRegistrationConfiguration) {

		this.log = checkNotNull(log);
		this.rpcService = checkNotNull(rpcService);
		this.targetName = checkNotNull(targetName);
		this.targetType = checkNotNull(targetType);
		this.targetAddress = checkNotNull(targetAddress);
		this.fencingToken = checkNotNull(fencingToken);
		this.retryingRegistrationConfiguration = checkNotNull(retryingRegistrationConfiguration);

		this.completionFuture = new CompletableFuture<>();
	}

	// ------------------------------------------------------------------------
	//  completion and cancellation
	// ------------------------------------------------------------------------

	public CompletableFuture<Tuple2<G, S>> getFuture() {
		return completionFuture;
	}

	/**
	 * Cancels the registration procedure.
	 */
	public void cancel() {
		canceled = true;
		completionFuture.cancel(false);
	}

	/**
	 * Checks if the registration was canceled.
	 * @return True if the registration was canceled, false otherwise.
	 */
	public boolean isCanceled() {
		return canceled;
	}

	// ------------------------------------------------------------------------
	//  registration
	// ------------------------------------------------------------------------

	protected abstract CompletableFuture<RegistrationResponse> invokeRegistration(
		G gateway, F fencingToken, long timeoutMillis) throws Exception;

	/**
	 * This method resolves the target address to a callable gateway and starts the
	 * registration after that.
	 */
	@SuppressWarnings("unchecked")
	public void startRegistration() {
		if (canceled) {
			// we already got canceled
			return;
		}

		try {
			// trigger resolution of the target address to a callable gateway
			final CompletableFuture<G> rpcGatewayFuture;

			if (FencedRpcGateway.class.isAssignableFrom(targetType)) {
				rpcGatewayFuture = (CompletableFuture<G>) rpcService.connect(
					targetAddress,
					fencingToken,
					targetType.asSubclass(FencedRpcGateway.class));
			} else {
				rpcGatewayFuture = rpcService.connect(targetAddress, targetType);
			}

			// upon success, start the registration attempts
			CompletableFuture<Void> rpcGatewayAcceptFuture = rpcGatewayFuture.thenAcceptAsync(
				(G rpcGateway) -> {
					log.info("Resolved {} address, beginning registration", targetName);
					register(rpcGateway, 1, retryingRegistrationConfiguration.getInitialRegistrationTimeoutMillis());
				},
				rpcService.getExecutor());
		}
		catch (Throwable t) {
			completionFuture.completeExceptionally(t);
			cancel();
		}
	}

	/**
	 * This method performs a registration attempt and triggers either a success notification or a retry,
	 * depending on the result.
	 */
	@SuppressWarnings("unchecked")
	private void register(final G gateway, final int attempt, final long timeoutMillis) {
		// eager check for canceling to avoid some unnecessary work
		if (canceled) {
			return;
		}

		try {
			log.debug("Registration at {} attempt {} (timeout={}ms)", targetName, attempt, timeoutMillis);
			CompletableFuture<RegistrationResponse> registrationFuture = invokeRegistration(gateway, fencingToken, timeoutMillis);

			// if the registration was successful, let the TaskExecutor know
			CompletableFuture<Void> registrationAcceptFuture = registrationFuture.thenAcceptAsync(
				(RegistrationResponse result) -> {
					if (!isCanceled()) {
						// registration successful!
						S success = (S) result;
						completionFuture.complete(Tuple2.of(gateway, success));
					}
				},
				rpcService.getExecutor());

			// upon failure, retry
			registrationAcceptFuture.whenCompleteAsync(
				(Void v, Throwable failure) -> {},
				rpcService.getExecutor());
		}
		catch (Throwable t) {
			completionFuture.completeExceptionally(t);
			cancel();
		}
	}

	private void registerLater(final G gateway, final int attempt, final long timeoutMillis, long delay) {
		rpcService.scheduleRunnable(new Runnable() {
			@Override
			public void run() {
				register(gateway, attempt, timeoutMillis);
			}
		}, delay, TimeUnit.MILLISECONDS);
	}

}
