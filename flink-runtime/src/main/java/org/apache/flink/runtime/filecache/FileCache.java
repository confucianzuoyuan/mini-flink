/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.filecache;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.blob.PermanentBlobService;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.ShutdownHookUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The FileCache is used to access registered cache files when a task is deployed.
 *
 * <p>Files and zipped directories are retrieved from the {@link PermanentBlobService}. The life-cycle of these files
 * is managed by the blob-service.
 *
 * <p>Retrieved directories will be expanded in "{@code <system-tmp-dir>/tmp_<jobID>/}"
 * and deleted when the task is unregistered after a 5 second delay, unless a new task requests the file in the meantime.
 */
public class FileCache {

	private static final Logger LOG = LoggerFactory.getLogger(FileCache.class);

	/** cache-wide lock to ensure consistency. copies are not done under this lock. */
	private final Object lock = new Object();

	private final Map<JobID, Map<String, Future<Path>>> entries;

	private final Map<JobID, Set<ExecutionAttemptID>> jobRefHolders;

	private final ScheduledExecutorService executorService;

	private final File[] storageDirectories;

	private final Thread shutdownHook;

	private int nextDirectory;

	private final PermanentBlobService blobService;

	private final long cleanupInterval; //in milliseconds

	// ------------------------------------------------------------------------

	public FileCache(String[] tempDirectories, PermanentBlobService blobService) throws IOException {
		this (tempDirectories, blobService, Executors.newScheduledThreadPool(10,
			new ExecutorThreadFactory("flink-file-cache")), 5000);
	}

	@VisibleForTesting
	FileCache(String[] tempDirectories, PermanentBlobService blobService,
		ScheduledExecutorService executorService, long cleanupInterval) throws IOException {

		Preconditions.checkNotNull(tempDirectories);
		this.cleanupInterval = cleanupInterval;

		storageDirectories = new File[tempDirectories.length];

		for (int i = 0; i < tempDirectories.length; i++) {
			String cacheDirName = "flink-dist-cache-" + UUID.randomUUID().toString();
			storageDirectories[i] = new File(tempDirectories[i], cacheDirName);
			String path = storageDirectories[i].getAbsolutePath();

			if (storageDirectories[i].mkdirs()) {
				LOG.info("User file cache uses directory " + path);
			} else {
				LOG.error("User file cache cannot create directory " + path);
				// delete all other directories we created so far
				for (int k = 0; k < i; k++) {
					if (!storageDirectories[k].delete()) {
						LOG.warn("User file cache cannot remove prior directory " +
								storageDirectories[k].getAbsolutePath());
					}
				}
				throw new IOException("File cache cannot create temp storage directory: " + path);
			}
		}

		this.shutdownHook = createShutdownHook(this, LOG);

		this.entries = new HashMap<>();
		this.jobRefHolders = new HashMap<>();
		this.executorService = executorService;
		this.blobService = blobService;
	}

	/**
	 * Shuts down the file cache by cancelling all.
	 */
	public void shutdown() {
		synchronized (lock) {
			// first shutdown the thread pool
			ScheduledExecutorService es = this.executorService;
			if (es != null) {
				es.shutdown();
				try {
					es.awaitTermination(cleanupInterval, TimeUnit.MILLISECONDS);
				}
				catch (InterruptedException e) {
					// may happen
				}
			}

			entries.clear();
			jobRefHolders.clear();

			ShutdownHookUtil.removeShutdownHook(shutdownHook, getClass().getSimpleName(), LOG);
		}
	}



	private static Thread createShutdownHook(final FileCache cache, final Logger logger) {

		return ShutdownHookUtil.addShutdownHook(
			cache::shutdown,
			FileCache.class.getSimpleName(),
			logger
		);
	}

	public void releaseJob(JobID jobId, ExecutionAttemptID executionId) {
		checkNotNull(jobId);

		synchronized (lock) {
			Set<ExecutionAttemptID> jobRefCounter = jobRefHolders.get(jobId);

			if (jobRefCounter == null || jobRefCounter.isEmpty()) {
				return;
			}

			jobRefCounter.remove(executionId);
			if (jobRefCounter.isEmpty()) {
				executorService.schedule(new DeleteProcess(jobId), cleanupInterval, TimeUnit.MILLISECONDS);
			}
		}
	}

	// ------------------------------------------------------------------------
	//  background processes
	// ------------------------------------------------------------------------

	/**
	 * If no task is using this file after 5 seconds, clear it.
	 */
	@VisibleForTesting
	class DeleteProcess implements Runnable {

		private final JobID jobID;

		DeleteProcess(JobID jobID) {
			this.jobID = jobID;
		}

		@Override
		public void run() {
				synchronized (lock) {

					Set<ExecutionAttemptID> jobRefs = jobRefHolders.get(jobID);
					if (jobRefs != null && jobRefs.isEmpty()) {
						// abort the copy
						for (Future<Path> fileFuture : entries.get(jobID).values()) {
							fileFuture.cancel(true);
						}

						//remove job specific entries in maps
						entries.remove(jobID);
						jobRefHolders.remove(jobID);

					}
				}
		}
	}
}
