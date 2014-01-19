package de.uni_stuttgart.ipvs.ids.communication;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.Vector;

/**
 * Part b) Extend the method receiveMessages to return all DatagramPackets that
 * were received during the given timeout.
 * 
 * Also implement unpack() to conveniently convert a Collection of
 * DatagramPackets containing ValueResponseMessages to a collection of
 * MessageWithSource objects.
 * 
 */
public class NonBlockingReceiver {

	protected DatagramSocket socket;

	public final int buf_length = 1024;

	public NonBlockingReceiver(DatagramSocket socket) {
		this.socket = socket;
	}

	public Vector<DatagramPacket> receiveMessages(int timeoutMillis,
			int expectedMessages) throws IOException {
		// TODO: Implement me!
		System.out.println("Trying to receive " + expectedMessages + " in "
				+ timeoutMillis);

		Vector<DatagramPacket> packets = new Vector<DatagramPacket>();

		

		// Rest of time and number of received packets.

		double rest = timeoutMillis;
		int received = 0;
		
		int interval = (int) (rest / (expectedMessages - received));

		while ((rest > 0) && received < expectedMessages) {

			// Calculate the length of the new interval
			DatagramPacket packet = new DatagramPacket(new byte[buf_length],
					buf_length);

			try {
				long current_time = System.currentTimeMillis();
				socket.setSoTimeout(interval);
				socket.receive(packet);
				// Time to receive the packet. Update rest of time

				rest -= (int) (System.currentTimeMillis() - current_time);
				// Add to the list
				packets.add(packet);
				received++;

				interval = (int) (rest / (expectedMessages - received));
			} catch (SocketTimeoutException e) {
				rest -= interval;
			}


		}
		return packets;
	}

	public static <T> Collection<MessageWithSource<T>> unpack(
			Collection<DatagramPacket> packetCollection) throws IOException,
			ClassNotFoundException {
		// TODO: Implement me!

		Set<MessageWithSource<T>> msgs = new HashSet<MessageWithSource<T>>();

		for (DatagramPacket p : packetCollection) {
			MessageWithSource<T> msg = new MessageWithSource<T>(
					p.getSocketAddress(), (T) getObjectFromMessage(p));
			msgs.add(msg);

		}

		return msgs;

	}

	private static Object getObjectFromMessage(DatagramPacket packet)
			throws IOException, ClassNotFoundException {
		// TODO: Implement me!.

		ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(
				packet.getData()));
		Object obj = in.readObject();

		return obj;

	}

}
