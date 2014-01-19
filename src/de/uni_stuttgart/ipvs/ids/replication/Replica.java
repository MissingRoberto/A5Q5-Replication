package de.uni_stuttgart.ipvs.ids.replication;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.util.concurrent.ThreadFactory;

import de.uni_stuttgart.ipvs.ids.communication.ReadRequestMessage;
import de.uni_stuttgart.ipvs.ids.communication.ReleaseReadLock;
import de.uni_stuttgart.ipvs.ids.communication.ReleaseWriteLock;
import de.uni_stuttgart.ipvs.ids.communication.RequestReadVote;
import de.uni_stuttgart.ipvs.ids.communication.RequestWriteVote;
import de.uni_stuttgart.ipvs.ids.communication.ValueResponseMessage;
import de.uni_stuttgart.ipvs.ids.communication.Vote;
import de.uni_stuttgart.ipvs.ids.communication.WriteRequestMessage;

public class Replica<T> extends Thread {

	public enum LockType {
		UNLOCKED, READLOCK, WRITELOCK
	};

	private int id;

	private double availability;
	private VersionedValue<T> value;

	protected DatagramSocket socket = null;

	protected LockType lock;

	private final int length_buf = 1024;

	/**
	 * This address holds the addres of the client holding the lock. This
	 * variable should be set to NULL every time the lock is set to UNLOCKED.
	 */
	protected SocketAddress lockHolder;

	public Replica(int id, int listenPort, double availability, T initialValue)
			throws SocketException {
		super("Replica:" + listenPort);
		this.id = id;
		SocketAddress socketAddress = new InetSocketAddress("127.0.0.1",
				listenPort);
		this.socket = new DatagramSocket(socketAddress);
		this.availability = availability;
		this.value = new VersionedValue<T>(0, initialValue);
		this.lock = LockType.UNLOCKED;
	}

	/**
	 * Part a) Implement this run method to receive and process request
	 * messages. To simulate a replica that is sometimes unavailable, it should
	 * randomly discard requests as long as it is not locked. The probability
	 * for discarding a request is (1 - availability).
	 * 
	 * For each request received, it must also be checked whether the request is
	 * valid. For example: - Does the requesting client hold the correct lock? -
	 * Is the replica unlocked when a new lock is requested?
	 */
	public void run() {
		// TODO: Implement me!

		// IMPORTANT. I have supposed that the availability doesn't affect
		// to the vote requests to avoid the problem that the service is not
		// available after the majority consensus or not blocking the replicas
		// after the transaction.

		byte[] buf = new byte[length_buf];

		// NO PUEDO ATENDER A VARIOS CLIENTES.

		while (true) {

			try {
				DatagramPacket packet = new DatagramPacket(buf, length_buf);
				socket.receive(packet);

				ClientThread client = new ClientThread(packet);
				client.start();

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	/**
	 * This is a helper method. You can implement it if you want to use it or
	 * just ignore it. Its purpose is to send a Vote (YES/NO depending on the
	 * state) to the given address.
	 */
	protected void sendVote(SocketAddress address, Vote.State state, int version)
			throws IOException {
		// TODO: Implement me!

		sendObject(address, new Vote(state, version));

	}

	protected void sendObject(SocketAddress address, Object obj)
			throws IOException {
		// TODO: Implement me!

		// Write the object in a byte array buffer

		ByteArrayOutputStream bytes_out = new ByteArrayOutputStream(length_buf);
		ObjectOutputStream out = new ObjectOutputStream(bytes_out);
		out.writeObject(obj);

		// Send the object
		socket.send(new DatagramPacket(bytes_out.toByteArray(), bytes_out
				.size(), address));

	}

	/**
	 * This is a helper method. You can implement it if you want to use it or
	 * just ignore it. Its purpose is to extract the object stored in a
	 * DatagramPacket.
	 */
	protected Object getObjectFromMessage(DatagramPacket packet)
			throws IOException, ClassNotFoundException {

		// TODO: Implement me!

		ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(
				packet.getData()));
		Object obj = in.readObject();

		// If it isn't instance of any of the expected message, throw exception.

		if (obj instanceof RequestReadVote || obj instanceof RequestWriteVote
				|| obj instanceof ReadRequestMessage
				|| obj instanceof WriteRequestMessage<?>
				|| obj instanceof ReleaseReadLock
				|| obj instanceof ReleaseWriteLock)
			return obj;

		else
			throw new ClassCastException();

	}

	public int getID() {
		return id;
	}

	public SocketAddress getSocketAddress() {
		return socket.getLocalSocketAddress();
	}

	
	public class ClientThread extends Thread {

		private DatagramPacket packet;

		public ClientThread(DatagramPacket packet) {
			// store parameter for later user
			this.packet = packet;
		}

		public void run() {
			InetSocketAddress dest = (InetSocketAddress) packet
					.getSocketAddress();

			// I read the data object.

			try {
				Object obj = getObjectFromMessage(packet);

				if (Math.random() < availability) {

					// I am available to give a vote.

					if (obj instanceof RequestReadVote) {
						if (!lock.equals(LockType.WRITELOCK)) {
							// if is not write locked, lock read,
							// and return
							// a positive vote

							lock = LockType.READLOCK;
							sendVote(dest, Vote.State.YES, value.getVersion());
						} else {
							// Send Negative vote
							sendVote(dest, Vote.State.NO, value.getVersion());
						}

					} else if (obj instanceof RequestWriteVote) {

						if (lock.equals(LockType.UNLOCKED)) {
							// if is not write locked, lock read,
							// and return
							// a positive vote

							lock = LockType.WRITELOCK;
							sendVote(dest, Vote.State.YES, value.getVersion());
						} else {
							// Send Negative vote
							sendVote(dest, Vote.State.NO, value.getVersion());
						}

					}

				}
				if (obj instanceof ReadRequestMessage) {
					// Send the value
					ValueResponseMessage<T> ms = new ValueResponseMessage<T>(
							value.getValue());

					sendObject(dest, ms);

				} else if (obj instanceof WriteRequestMessage<?>) {
					WriteRequestMessage<T> write_ms = (WriteRequestMessage<T>) obj;

					if (write_ms.getVersion() > value.getVersion()) {
						value = new VersionedValue<T>(write_ms.getVersion(),
								write_ms.getValue());

						// Send ACK
						sendVote(dest, Vote.State.YES, value.getVersion());
					} else {
						// Send NACK
						sendVote(dest, Vote.State.NO, value.getVersion());
					}

				} else if (obj instanceof ReleaseReadLock) {
					lock = LockType.UNLOCKED;
					// Send ACK
					sendVote(dest, Vote.State.YES, value.getVersion());
				} else if (obj instanceof ReleaseWriteLock) {
					lock = LockType.UNLOCKED;

					// Send ACK
					sendVote(dest, Vote.State.YES, value.getVersion());

				}

			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO: handle exception
				e.printStackTrace();
			}
		}
	}

}
