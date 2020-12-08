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

package org.apache.flink.runtime.blob;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Socket;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import static org.apache.flink.runtime.blob.BlobUtils.closeSilently;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A BLOB connection handles a series of requests from a particular BLOB client.
 */
class BlobServerConnection extends Thread {

	/** The log object used for debugging. */
	private static final Logger LOG = LoggerFactory.getLogger(BlobServerConnection.class);

	/** The socket to communicate with the client. */
	private final Socket clientSocket;

	/** The BLOB server. */
	private final BlobServer blobServer;

	/** Read lock to synchronize file accesses. */
	private final Lock readLock;

	/**
	 * Creates a new BLOB connection for a client request.
	 *
	 * @param clientSocket The socket to read/write data.
	 * @param blobServer The BLOB server.
	 */
	BlobServerConnection(Socket clientSocket, BlobServer blobServer) {
		super("BLOB connection for " + clientSocket.getRemoteSocketAddress());
		setDaemon(true);

		this.clientSocket = clientSocket;
		this.blobServer = checkNotNull(blobServer);

		ReadWriteLock readWriteLock = blobServer.getReadWriteLock();

		this.readLock = readWriteLock.readLock();
	}

	// --------------------------------------------------------------------------------------------
	//  Connection / Thread methods
	// --------------------------------------------------------------------------------------------

	/**
	 * Main connection work method. Accepts requests until the other side closes the connection.
	 */
	@Override
	public void run() {
	}

	/**
	 * Closes the connection socket and lets the thread exit.
	 */
	public void close() {
		closeSilently(clientSocket, LOG);
		interrupt();
	}
}
