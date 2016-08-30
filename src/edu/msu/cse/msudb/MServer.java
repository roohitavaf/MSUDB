package edu.msu.cse.msudb;

import java.io.File;
import java.io.FileInputStream;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.AsciiHeadersEncoder.NewlineType;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.CharsetUtil;

public class MServer {
	public static int maxMessageSize = 1024 * 1024;

	static byte[] e = { 0x03 };
	public static ByteBuf messageDelimiter = Unpooled.copiedBuffer(e);
	static char etx = (char) 3;
	public static Bootstrap bPartitionsSend; // We only need this bootstrap as static, others will be only used one time in the main method.

	//Clinet communication is not via Netty and it use multi-threaded synchronous communication
	public static int numberOfClientThreads = 100;

	public static int clientPort;
	public static int coordinationPort;
	public static String DBdirectory;
	public static Environment env;
	public static Database db;

	//A hashMap of all channels opened to other servers
	public static ConcurrentHashMap<String, Channel> partitionChannels = new ConcurrentHashMap<String, Channel>();

	public static HashMap<String, String> ipTable = new HashMap<>(); //<"dc,pn", integer>
	public static HashMap<String, Integer> portTable = new HashMap<>();

	//Special characters 
	public static String mainDelimiter = ":";
	public static String interDepItemDelimiter = ";";
	public static String intraDepItemDelimiter = ",";
	public static String idDelimiter = ",";
	public static String emptyElement = "~";

	//Other configuration
	public static String hashAlgorithm = "SHA-256";

	//binders
	public static RecordBinder rb = new RecordBinder();

	//MSU-DB protocl.................
	public static int dcn = -1; //dc number
	public static int pn = -1; //partition number
	public static int numberOfDcs = -1; //number of datacenters in the system
	public static int numberPartitionsPerDC = -1;

	//HLC clock and its lock
	public static long hlc;
	public static Object hlcLock = new Object();

	//collections
	public static List<Long> vv = Collections.synchronizedList(new ArrayList<Long>());
	public static Object vvLock = new Object();

	//DSV
	public static ArrayList<Long> DSV = new ArrayList<Long>();

	//Timers
	public static int heartbeatInterval = 10; //(ms)
	public static ScheduledExecutorService heartbeatTimer = Executors.newScheduledThreadPool(1);
	public static int dsvComputationInterval = 5000;
	public static ScheduledExecutorService gstComputationTimer = Executors.newScheduledThreadPool(1);
	public static long lastReplicateTime = -1;
	public static Object lastReplcateTimeLock = new Object();

	//Tree
	public static int parent = -1;
	public static ArrayList<Integer> children = new ArrayList<Integer>();
	public static HashMap<Integer, ArrayList<Long>> childrenVVs = new HashMap<Integer, ArrayList<Long>>();

	//Test variables: 
	public static int clockSkew = 0;
	public static int networkDelay = 200; //in milliseconds 
	public static long updateTimeOfCreation;
	public static float avargeUVL = 0;
	public static int numberOfReplicatedMessage = 0;
	public static int maxNumberOfRepliateMessages = 20;
	public static boolean firstReplicate = true;


	//Mirco test
	public static boolean collect = false;
	public static int numberOfOperations = 0;
	public static ScheduledExecutorService microStarterTimer = Executors.newScheduledThreadPool(1);
	public static ScheduledExecutorService microTerminatorTimer = Executors.newScheduledThreadPool(1);

	//-------------------------------------------------

	public static void main(String args[]) throws Exception {

		//initial setup
		readConfigFile(args);

		//initiateing vv
		for (int i = 0; i < numberOfDcs; i++) {
			vv.add(new Long(-1));
			DSV.add(new Long(-1));

		}
		System.out.println("GServer: vv size " + vv.size());

		//Runing DBs
		runDBs();

		//Running timers
		heartbeatTimer.scheduleAtFixedRate(new HeartbeatSender(), 0, heartbeatInterval, TimeUnit.MILLISECONDS);
		gstComputationTimer.scheduleAtFixedRate(new DSVComputation(), 0, dsvComputationInterval, TimeUnit.MILLISECONDS);

		//Test......
		//Running the server..
		//run the client server in another thread:
		Thread t = new Thread(new ClientListener());
		t.start();

		//running server for paritions
		new MServer().run();
	}

	public static void readConfigFile(String[] args) {
		try //all of this will chnage to use a config file in future. 
		{
			String configFile = new String(args[0]);

			String c_ip = new String(args[1]);
			int c_port = new Integer(args[2]);
			coordinationPort = c_port;
			clientPort = c_port + 1;
			clockSkew = new Integer(args[3]);
			networkDelay = new Integer(args[4]);

			String rootDBFolder = args[5];

			//Test......
			int startDelay = new Integer(args[6]);
			int expTime = new Integer(args[7]);
			microStarterTimer.scheduleWithFixedDelay(new MicroStarter(), startDelay * 1000, startDelay * 1000,
					TimeUnit.MILLISECONDS);
			microTerminatorTimer.scheduleWithFixedDelay(new MicroTerminator(expTime), (startDelay + expTime) * 1000,
					(startDelay + expTime) * 1000, TimeUnit.MILLISECONDS);
			
			//creating ipTable 
			File file = new File(configFile);
			FileInputStream fis = new FileInputStream(file);
			byte[] data = new byte[(int) file.length()];
			fis.read(data);
			fis.close();

			String configContent = new String(data, "UTF-8");

			String[] configContentParts = configContent.split(";");
			numberOfDcs = new Integer(configContentParts[0]);
			numberPartitionsPerDC = (configContentParts.length - 1) / numberOfDcs;

			for (int d = 0; d < numberOfDcs; d++) {
				for (int p = 0; p < numberPartitionsPerDC; p++) {
					int index = d * numberPartitionsPerDC + p + 1;
					String nodeDescription = configContentParts[index];

					String[] nodeDescriptionParts = nodeDescription.split(mainDelimiter);

					//format of each entry in the file: id:ip:port:parent_id;
					int f_id = new Integer(nodeDescriptionParts[0].trim());
					String f_ip = nodeDescriptionParts[1].trim();
					Integer f_port = new Integer(nodeDescriptionParts[2].trim());
					int f_parent_id = new Integer(nodeDescriptionParts[3].trim());

					if (p != f_id)
						System.out.println("Problem in the config file: id is wrong!"); //just to make sure, as we don't id. 
					String key = d + idDelimiter + p;
					ipTable.put(key, f_ip);
					portTable.put(key, f_port);
					parent = f_parent_id;

					if (f_ip.equals(c_ip) && f_port == c_port) {
						dcn = d;
						pn = p;
						DBdirectory =  rootDBFolder + "/DBs/DB" + d + "_" + p;
						
						//Just for Test.......We clean the DB directory
						File DBfolder = new File (DBdirectory);
						for(File fileToDelete: DBfolder.listFiles())
							fileToDelete.delete();
						//............
						parent = f_parent_id;

					}
					if (f_parent_id == pn && d == dcn && p != pn) {
						children.add(p);
						childrenVVs.put(p, new ArrayList<Long>());
					}

				}
			}

		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
	}

	public static void runDBs() {

		//creating database: 
		EnvironmentConfig conf = new EnvironmentConfig();
		conf.setAllowCreate(true);
		env = new Environment(new File(DBdirectory), conf);

		DatabaseConfig dbConfig = new DatabaseConfig();
		dbConfig.setAllowCreate(true);
		//allowing duplicates
		dbConfig.setSortedDuplicates(true);
		dbConfig.setDuplicateComparator((Class<? extends Comparator<byte[]>>) MainDBCompartor.class);

		db = env.openDatabase(null, "MainDB", dbConfig);

		/*
		 * //db for pending Keys DatabaseConfig dbConfig2 = new
		 * DatabaseConfig(); dbConfig2.setAllowCreate(true); //allowing
		 * duplicates dbConfig2.setSortedDuplicates(false); penDB =
		 * env.openDatabase(null, "PenDB", dbConfig2);
		 */
	}

	public void run() throws Exception {

		// Groups for bPartitionsReceive
		EventLoopGroup bossGroupForPartitionsReceive = new NioEventLoopGroup();
		EventLoopGroup workerGroupForPartitionsReceive = new NioEventLoopGroup();

		// Groups for bPartitionsSend
		EventLoopGroup workerGroupForPartitionsSend = new NioEventLoopGroup();

		try {

			// >>>>>>>>>>>>>>>Setting up the server that responds to parition
			// messages:
			ServerBootstrap bPartitionsReceive = new ServerBootstrap(); // (2)
			bPartitionsReceive.group(bossGroupForPartitionsReceive, workerGroupForPartitionsReceive)
					.channel(NioServerSocketChannel.class) // (3)
					.childHandler(new ChannelInitializer<SocketChannel>() { // (4)
						@Override
						public void initChannel(SocketChannel ch) throws Exception {

							ch.pipeline().addLast("frameDecoder",
									new DelimiterBasedFrameDecoder(maxMessageSize, messageDelimiter));
							ch.pipeline().addLast("stringDecoder", new StringDecoder(CharsetUtil.UTF_8));

							// Encoder
							ch.pipeline().addLast("stringEncoder", new StringEncoder(CharsetUtil.UTF_8));

							ch.pipeline().addLast(new PartitionHandler());
						}
					}).option(ChannelOption.SO_BACKLOG, 128).childOption(ChannelOption.SO_KEEPALIVE, true);

			// Bind and start to accept incoming connections.
			ChannelFuture fPartitionsReceive = bPartitionsReceive.bind(coordinationPort).sync();

			// >>>>>>>>>>>>>Creating the bootstrap that is used when we want to
			// make a new connection to another parition
			bPartitionsSend = new Bootstrap(); // (1)
			bPartitionsSend.group(workerGroupForPartitionsSend); // (2)
			bPartitionsSend.channel(NioSocketChannel.class); // (3)
			bPartitionsSend.option(ChannelOption.SO_KEEPALIVE, true); // (4)
			bPartitionsSend.handler(new ChannelInitializer<SocketChannel>() {
				@Override
				public void initChannel(SocketChannel ch) throws Exception {

					// Decoders
					ch.pipeline().addLast("frameDecoder",
							new DelimiterBasedFrameDecoder(maxMessageSize, messageDelimiter));
					ch.pipeline().addLast("stringDecoder", new StringDecoder(CharsetUtil.UTF_8));

					// Encoder
					ch.pipeline().addLast("stringEncoder", new StringEncoder(CharsetUtil.UTF_8));
					ch.pipeline().addLast(new PartitionHandler());
				}
			});
			// Now, bPartitionsSend is ready for use to create new connection to
			// other paritions.

			System.out.println("Server is running...");
			System.out.println("ID: dcn=" + dcn + " pn=" + pn);
			System.out.println("Listening for servers on port " + coordinationPort);
			System.out.println("Listening for clients on port " + clientPort);

			// We close server channels.
			fPartitionsReceive.channel().closeFuture().sync();

		} finally {

			bossGroupForPartitionsReceive.shutdownGracefully();
			workerGroupForPartitionsReceive.shutdownGracefully();
			workerGroupForPartitionsSend.shutdownGracefully();
		}
	}

	//Utilities

	//clock methods: 
	public static long getCurrentTime() {
		return System.currentTimeMillis() + clockSkew;
	}

	public static Record findTheNewestStableVersion(String key) {
		System.out.println("Inside Mserver, findTheNewst");
		Cursor cursor = null;
		try {

			// Geting the data form the DB
			DatabaseEntry myKey = new DatabaseEntry(getHash(key));
			DatabaseEntry myData = new DatabaseEntry();
			// Open a cursor using a database handle
			cursor = db.openCursor(null, null);
			// Position the cursor
			OperationStatus retVal = cursor.getSearchKey(myKey, myData, LockMode.DEFAULT);

			while (retVal == OperationStatus.SUCCESS) {
				Record record = (Record) rb.entryToObject(myData);
				System.out.println("Insdie Mserver: " + new String(record.value));
				// check if the found data is good (non-pending and for the
				// valied key):
				if (record.key.equals(key) && (record.sr == dcn)) {
					return record;
				}
				boolean visible = true;
				for (Map.Entry<Integer, Long> dvItem : record.dv.entrySet()) {
					if (dvItem.getValue() > DSV.get(dvItem.getKey())) {
						System.out.println(
								"Inside MServer: Value = " + new String(record.value) + " rected as dvItem.getValue()= "
										+ dvItem.getValue() + " and dsv value is " + DSV.get(dvItem.getKey()));
						visible = false;
						break;
					}
				}
				if (record.key.equals(key) && visible)
					return record;
				else {
					retVal = cursor.getNextDup(myKey, myData, LockMode.DEFAULT);
				}

			}
			cursor.close();
			return null;
		} catch (Exception exp) {
			exp.printStackTrace();
			if (cursor != null)
				cursor.close();
			return null;

		}
	}

	private static int lookupPort(int dcn_a, int pn_a) {
		return portTable.get(dcn_a + idDelimiter + pn_a);
	}

	private static String lookupIP(int dcn_a, int pn_a) {
		return ipTable.get(dcn_a + idDelimiter + pn_a);
	}

	public static void sendToPartition(int dcn_a, int pn_a, String mes_a) {

		String iP = lookupIP(dcn_a, pn_a);
		int port = lookupPort(dcn_a, pn_a);

		//System.out.println("Request for sending this message: " + mes_a);
		//Test......
		
		 try { Thread.sleep(networkDelay); } catch (InterruptedException e1) {
		  e1.printStackTrace(); }
		 
		String key = dcn_a + idDelimiter + pn_a;
		if (partitionChannels.containsKey(key) && partitionChannels.get(key).isActive()) {
			partitionChannels.get(key).writeAndFlush(mes_a + etx);
		} else {

			//System.out.println("Creating Channel for IP= " + iP + " port_a= " + port);
			ChannelFuture f;
			try {
				f = bPartitionsSend.connect(iP, port).sync();
				f.channel().writeAndFlush("ID" + mainDelimiter + dcn + idDelimiter + pn + etx);
				f.channel().writeAndFlush(mes_a + etx);
				partitionChannels.put(key, f.channel());
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (Exception e) {
				// TODO: handle exception
				//e.printStackTrace();
			}

		}

	}

	public static byte[] getHash(String str) throws NoSuchAlgorithmException, UnsupportedEncodingException {
		// calculating the hash value of the key:
		MessageDigest md = MessageDigest.getInstance(MServer.hashAlgorithm);
		md.update(str.getBytes("UTF-8")); // Change this to "UTF-16" if
		// needed
		byte[] digest = md.digest();

		return digest;
	}

	//HLC functions:
	public static long getL(long time) {
		return time & 0xFFFFFFFFFFFF0000L;
	}

	public static long getC(long time) {
		return time & 0x000000000000FFFFL;
	}

	public static long incrementL(long time) {
		return shiftToHighBits(1) + time;
	}

	public static long shitfBack(long time) {
		return time >>> 16;
	}

	public static long shiftToHighBits(long time) {
		return time << 16;
	}

	//this funciton if used to creating new timestamp for the heartbeat and local puts. 
	//Note thet it update the hlc as well. 
	public static long creatNewTimestamp() {
		return 0;
		/*
		long physicalClock = shiftToHighBits(getCurrentTime());
		synchronized (hlcLock) {
			long previousL = getL(hlc);
			long c = getC(hlc);
			long l = Long.max(previousL, physicalClock);
			if (l == previousL)
				c++;
			else
				c = 0;
			hlc = l + c;
			return hlc;
		}*/
	}

	//this funciton if used to advances the clock upon receiving a new replicate
	//Note thet it update the hlc as well. 
	public static long creatNewTimestamp(long mesHlc) {
		return 0; 
		/*
		long physicalClock = shiftToHighBits(getCurrentTime());
		long mesL = getL(mesHlc);
		long mesC = getC(mesHlc);

		synchronized (hlcLock) {
			long previousL = getL(hlc);
			long c = getC(hlc);
			long l = Long.max(Long.max(previousL, physicalClock), mesL);
			if (l == previousL && l == mesL)
				c = Long.max(c, mesC) + 1;
			else if (l == previousL)
				c = c + 1;
			else if (l == mesL)
				c = mesC + 1;
			else
				c = 0;

			hlc = l + c;
			return hlc;
		}*/
	}

}
