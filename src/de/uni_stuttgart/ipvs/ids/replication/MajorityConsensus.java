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
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.TimeoutException;

import de.uni_stuttgart.ipvs.ids.communication.MessageWithSource;
import de.uni_stuttgart.ipvs.ids.communication.NonBlockingReceiver;
import de.uni_stuttgart.ipvs.ids.communication.ReadRequestMessage;
import de.uni_stuttgart.ipvs.ids.communication.ReleaseReadLock;
import de.uni_stuttgart.ipvs.ids.communication.ReleaseWriteLock;
import de.uni_stuttgart.ipvs.ids.communication.RequestReadVote;
import de.uni_stuttgart.ipvs.ids.communication.RequestWriteVote;
import de.uni_stuttgart.ipvs.ids.communication.ValueResponseMessage;
import de.uni_stuttgart.ipvs.ids.communication.Vote;
import de.uni_stuttgart.ipvs.ids.communication.Vote.State;
import de.uni_stuttgart.ipvs.ids.communication.WriteRequestMessage;

public class MajorityConsensus<T> {

	protected Collection<SocketAddress> replicas;

	protected DatagramSocket socket;
	protected NonBlockingReceiver nbio;

	final static int TIMEOUT = 1000;

	private final int buf_length = 1024;

	public MajorityConsensus(Collection<SocketAddress> replicas)
			throws SocketException {
		this.replicas = replicas;
		SocketAddress address = new InetSocketAddress("127.0.0.1", 4999);
		this.socket = new DatagramSocket(address);
		this.nbio = new NonBlockingReceiver(socket);
	}

	/**
	 * Part c) Implement this method.
	 */
	protected Collection<MessageWithSource<Vote>> requestReadVote()
			throws QuorumNotReachedException {
		// TODO: Implement me!

		Collection<MessageWithSource<Vote>> votes = null;
		try {
			for (SocketAddress replica : replicas) {

				sendObject(replica, (Object) new RequestReadVote());
			}
			Vector<DatagramPacket> packets = nbio.receiveMessages(TIMEOUT,
					replicas.size());
			votes = NonBlockingReceiver.unpack(packets);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO: handle exception

			e.printStackTrace();
		}
		return votes;

	}

	/**
	 * Part c) Implement this method.
	 */
	protected void releaseReadLock(Collection<SocketAddress> lockedReplicas) {
		// TODO: Implement me!

		try {
			for (SocketAddress lockedReplica : lockedReplicas) {

				sendObject(lockedReplica, (Object) new ReleaseReadLock());
				socket.setSoTimeout(TIMEOUT);
			}

			Vector<DatagramPacket> datagrams = nbio.receiveMessages(TIMEOUT,
					lockedReplicas.size());

			for (DatagramPacket packet : datagrams) {
				Vote ack = (Vote) getObjectFromMessage(packet);
				System.out.println("ACK UNLOCKED ? "
						+ ack.getState().toString() + " from "
						+ packet.getSocketAddress());
			}

		} catch (IOException e) {

			e.printStackTrace();
		} catch (ClassNotFoundException e) {

			e.printStackTrace();
		}

	}

	/**
	 * Part d) Implement this method.
	 */
	protected Collection<MessageWithSource<Vote>> requestWriteVote()
			throws QuorumNotReachedException {
		// TODO: Implement me!
		Collection<MessageWithSource<Vote>> votes = null;
		try {
			for (SocketAddress replica : replicas) {
				sendObject(replica, (Object) new RequestWriteVote());
			}
			Vector<DatagramPacket> packets = nbio.receiveMessages(TIMEOUT,
					replicas.size());
			votes = NonBlockingReceiver.unpack(packets);

		} catch (IOException e) {
			
		} catch (ClassNotFoundException e) {
			// TODO: handle exception

			e.printStackTrace();
		}
		return votes;

	}

	/**
	 * Part d) Implement this method.
	 */
	protected void releaseWriteLock(Collection<SocketAddress> lockedReplicas) {
		// TODO: Implement me!

		try {
			for (SocketAddress lockedReplica : lockedReplicas) {
				sendObject(lockedReplica, (Object) new ReleaseWriteLock());
				socket.setSoTimeout(TIMEOUT);
			}

			Vector<DatagramPacket> datagrams = nbio.receiveMessages(TIMEOUT,
					lockedReplicas.size());

			for (DatagramPacket packet : datagrams) {
				Vote ack = (Vote) getObjectFromMessage(packet);
				System.out.println("ACK UNLOCKED? " + ack.getState().toString()
						+ " from " + packet.getSocketAddress());
			}

		} catch (IOException e) {

			e.printStackTrace();
		} catch (ClassNotFoundException e) {

			e.printStackTrace();
		}
	}

	/**
	 * Part c) Implement this method.
	 */
	protected T readReplica(SocketAddress replica) {
		// TODO: Implement me!
		System.out.println("Read replica-----");
		try {
			sendObject(replica, new ReadRequestMessage());
			DatagramPacket packet = new DatagramPacket(new byte[buf_length],
					buf_length, replica);
			socket.setSoTimeout(TIMEOUT);
			socket.receive(packet);
			ValueResponseMessage<T> value_msg = (ValueResponseMessage<T>) getObjectFromMessage(packet);
			System.out.println("Value read : " + value_msg.getValue());
			return value_msg.getValue();
		} catch (SocketException e) {
			System.out.println("Value couldn't be read");
		} catch (IOException e) {
			System.out.println("Value couldn't be read");
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

		return null;

	}

	/**
	 * Part d) Implement this method.
	 */
	protected void writeReplicas(Collection<SocketAddress> lockedReplicas,
			VersionedValue<T> newValue) {
		// TODO: Implement me!
		System.out.println("write replicas------");
		try {

			// Try to write in all the locked replicas
			for (SocketAddress rep : lockedReplicas) {
				sendObject(rep, new WriteRequestMessage<T>(newValue));
			}

			// Receive ACK
			nbio.receiveMessages(TIMEOUT, lockedReplicas.size());

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Part c) Implement this method (and checkQuorum(), see below) to read the
	 * replicated value using the majority consensus protocol.
	 */
	public VersionedValue<T> get() throws QuorumNotReachedException {
		// TODO: Implement me!
		System.out.println("GET------");
		VersionedValue<T> version_value = null;

		// Make a survey
		Collection<MessageWithSource<Vote>> replies = requestReadVote();

		// Get the yes votes.
		Collection<MessageWithSource<Vote>> lockeds = checkQuorum(replies);

		// Get a list with its addresses

		Collection<SocketAddress> lockeds_addr = getAddresses(lockeds);

		if (lockeds.size() > replies.size() / 2) {
			// If the yes, is majority, let's look for the highest version
			// value.
			int version = -1;
			SocketAddress node = null;
			for (MessageWithSource<Vote> locked : lockeds) {
				Vote vote = locked.getMessage();
				if (vote.getVersion() > version) {
					node = locked.getSource();
					version = locked.getMessage().getVersion();
				}
			}

			// Read the value from the replica
			T value = readReplica(node);
			if (value != null) {
				version_value = new VersionedValue<T>(version, value);
			}

		}

		// Release all the locked nodes.
		releaseReadLock(lockeds_addr);

		return version_value;
	}

	/**
	 * Part d) Implement this method to set the replicated value using the
	 * majority consensus protocol.
	 */
	public void set(T value) throws QuorumNotReachedException {
		// TODO: Implement me!
		System.out.println("SET------");
		// Make a survey
		Collection<MessageWithSource<Vote>> replies = requestWriteVote();

		// Get the yes votes.
		Collection<MessageWithSource<Vote>> lockeds = checkQuorum(replies);

		// Get a list with its addresses

		Collection<SocketAddress> lockeds_addr = getAddresses(lockeds);

		if (lockeds.size() >= replies.size() / 2) {
			// If the yes, is majority, let's look for the highest version
			// value.
			int version = -1;
			for (MessageWithSource<Vote> locked : lockeds) {
				Vote vote = locked.getMessage();
				if (vote.getVersion() > version) {
					version = locked.getMessage().getVersion();
				}
			}
			version += 1;
			// Write all the replicas.
			writeReplicas(lockeds_addr, new VersionedValue<T>(version, value));

		} else {
			System.out.println("It can't be written " + lockeds.size() + "  "
					+ replies.size() / 2);
		}

		// Release all the locked nodes.
		releaseWriteLock(lockeds_addr);

	}

	/**
	 * Part c) Implement this method to check whether a sufficient number of
	 * replies were received. If a sufficient number was received, this method
	 * should return the {@link MessageWithSource}s of the locked
	 * {@link Replica}s. Otherwise, a QuorumNotReachedException must be thrown.
	 * 
	 * @throws QuorumNotReachedException
	 */
	protected Collection<MessageWithSource<Vote>> checkQuorum(
			Collection<MessageWithSource<Vote>> replies)
			throws QuorumNotReachedException {
		// TODO: Implement me!

		int required = (int) (2 * (replicas.size() - replies.size()) + 1);

		Collection<MessageWithSource<Vote>> locked_replicas = new HashSet<MessageWithSource<Vote>>();

		if (replicas.size() >= required) {
			for (MessageWithSource<Vote> reply : replies) {
				if (reply.getMessage().getState().equals(Vote.State.YES))
					locked_replicas.add(reply);
			}
			return locked_replicas;
		} else {
			throw new QuorumNotReachedException(required, getAddresses(replies));
		}

	}

	private void sendObject(SocketAddress address, Object obj)
			throws IOException {
		// TODO: Implement me!

		// Write the object in a byte array buffer
		ByteArrayOutputStream bytes_out = new ByteArrayOutputStream(buf_length);
		ObjectOutputStream out = new ObjectOutputStream(bytes_out);
		out.writeObject(obj);

		// Send the object
		socket.send(new DatagramPacket(bytes_out.toByteArray(), bytes_out
				.size(), address));

	}

	private Object getObjectFromMessage(DatagramPacket packet)
			throws IOException, ClassNotFoundException {

		// TODO: Implement me!

		ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(
				packet.getData()));
		Object obj = in.readObject();

		return obj;

	}

	private Collection<SocketAddress> getAddresses(
			Collection<MessageWithSource<Vote>> messages) {
		Collection<SocketAddress> msg_addresses = new HashSet<SocketAddress>();

		for (MessageWithSource<Vote> msg : messages) {
			msg_addresses.add(msg.getSource());
		}
		return msg_addresses;
	}

}
