import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;

public class MasterServer {

	public static final int PORT = 33000;
	public static ConcurrentHashMap<String, DataNode> nodeMap;
	public static ConcurrentHashMap<String, DataNode> userMap;
	
	static final int MAX_CONNECTIONS = 10;
	static Queue<Socket> queue = new LinkedList<>();
	static Thread[] threads = new Thread[MAX_CONNECTIONS];

	public static enum FSActions {
		GET_FILES_BY_USER,
		RETRIEVE,
		STORE,
		REMOVE
	}

	public static void main(String[] args) {
	
		nodeMap = new ConcurrentHashMap<>();
		userMap = new ConcurrentHashMap<>();

		ServerSocket server = null;
		Socket client = null;
		BufferedReader br = null;
		
		try {

			// Read config file
			if(args.length != 1) {
				throw new Exception("Invalid arguments.");
			}
			String configPath = args[0];
			br = new BufferedReader(new FileReader(new File(configPath)));
			String line = "";
			while((line = br.readLine()) != null) {
	
				// Store in node map
				String[] values = line.split(" ");
				DataNode node = new DataNode(values[0], values[1], Integer.parseInt(values[2]));
				nodeMap.put(values[0], node);
				
				// Print available data nodes
				System.out.println("Data node: '" + values[0] + "' ONLINE / IP: " + values[1] + " / Port: " + values[2]);
				
			}
			br.close();
			
			// Start master server
			server = new ServerSocket(PORT);
			
			System.out.println("Master Server START: Port " + PORT);
			
			// Main server loop
			int id = 0;
			int threadCount = 0;
			while(true) {
				
				// Get client connection
				client = server.accept();
				System.out.println("Client connected: IP " + client.getInetAddress());
				
				// Add socket to queue
				if(threadCount < 10) {
					queue.add(client);
				} else {
					
				}
				
				// Read client action
				String action = LionFSUtil.readString(client);

				// Read username
				String username = LionFSUtil.readString(client);

				// Assign node to user if needed
				if(!userMap.containsKey(username)) {
					assignNodeToUser(username);
				}

				// Open socket with data node
				DataNode dataNode = userMap.get(username);

				// Process next socket
				Socket s = queue.poll();
				if(s != null && threadCount < threads.length) {
					Thread t = new Thread(new Worker(id, username, s, dataNode, action));
					threads[id++ % MAX_CONNECTIONS] = t;
					t.start();
					threadCount++;
				}

				// Update threads
				for(int i = 0; i < threads.length; i++) {
					Thread t = threads[i];
					if(t != null) {
						if(t.getState() == Thread.State.TERMINATED) {
							threads[i] = null;
							threadCount--;
						}
					}
				}
			}
		} catch(IOException e) {
			e.printStackTrace();
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if(client != null) client.close();
				if(br != null) br.close();
			} catch(IOException e) {
				e.printStackTrace();
			}
		}
	
	}
	
	public static void assignNodeToUser(String username) {
		
		// Get all nodes
		Collection<DataNode> nodes = nodeMap.values();
		
		// Select node with most capacity
		DataNode selection = null;
		for(DataNode node : nodes) {
			if(selection == null) {
				selection = node;
			} else {
				selection = node.least(selection);
			}			
		}
		
		userMap.put(username, selection);
		selection.setUserCount(selection.getUserCount() + 1);
		
	}

	public static String[] getFilesByUsername(DataNode node, String username) {

		List<String> fileNames = new ArrayList<>();
		Socket socket = null;
		try {

			socket = new Socket(node.getIp(), node.getPort());

			// Write action to data node
			LionFSUtil.writeString(socket, "GET_FILES_BY_USER");
			
			// Write username
			LionFSUtil.writeString(socket, username);
			
			// Get file list as response
			int fileCount = LionFSUtil.readInt(socket);
			for(int i = 0; i < fileCount; i++) {
				String fileName = LionFSUtil.readString(socket);
				fileNames.add(fileName);
			}	
		} catch(IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if(socket != null) socket.close();
			} catch(IOException e) {
				e.printStackTrace();
			}
		}

		return fileNames.toArray(new String[fileNames.size()]);
	}

	public static void storeFiles(DataNode node, List<FSFile> files, String username) {
	
		Socket socket = null;
		try {

			// Connect to data node
			socket = new Socket(node.getIp(), node.getPort());
			
			// Write action
			LionFSUtil.writeString(socket, "STORE");
			
			// Write username
			LionFSUtil.writeString(socket, username);
			
			// Write file count
			LionFSUtil.writeInt(socket, files.size());

			// Write all files
			for(int i = 0; i < files.size(); i++) {
				FSFile file = files.get(i);
				LionFSUtil.writeString(socket, file.getName());
				LionFSUtil.writeInt(socket, file.getSize());
				LionFSUtil.writeBytes(socket, file.getData());
			}

		} catch(IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if(socket != null) socket.close();
			} catch(IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static List<FSFile> retrieveFiles(DataNode node, String username, List<String> fileNames) {
		
		List<FSFile> files = new ArrayList<>();
		try {
	
			// Connect to data node
			Socket socket = new Socket(node.getIp(), node.getPort());
			
			// Write action
			LionFSUtil.writeString(socket, "RETRIEVE");

			// Write username
			LionFSUtil.writeString(socket, username);
			
			// Write file count
			LionFSUtil.writeInt(socket, fileNames.size());
			
			// Write file names
			for(int i = 0; i < fileNames.size(); i++) {
				LionFSUtil.writeString(socket, fileNames.get(i));
			}
		
			// Read files
			for(int i = 0; i < fileNames.size(); i++) {
				String name = LionFSUtil.readString(socket);
				int size = LionFSUtil.readInt(socket);
				byte[] fileb = LionFSUtil.readBytes(socket, size);
				FSFile file = new FSFile(name, size, fileb);
				files.add(file);
			}
			
		} catch(IOException e) {
			e.printStackTrace();
		}
		
		return files;
	}
	
	public static void removeFile(DataNode node, String username, String fileName) {
		
		Socket socket = null;
		try {
			
			// Connect to data node
			socket = new Socket(node.getIp(), node.getPort());
			
			// Write remove action
			LionFSUtil.writeString(socket, "REMOVE");
			
			// Write username
			LionFSUtil.writeString(socket, username);
			
			// Write file name
			LionFSUtil.writeString(socket, fileName);
			
		} catch(IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if(socket != null) socket.close();
			} catch(IOException e) {
				e.printStackTrace();
			}
		}
		
	}

}

class Worker implements Runnable {
	
	private int id;
	private String username;
	private Socket client;
	private DataNode dataNode;
	private FSAction action;

	public enum FSAction {
		GET_FILES_BY_USER,
		RETRIEVE,
		STORE,
		REMOVE
	}
	
	public Worker(int id, String username, Socket client, DataNode dataNode, String action) {
		this.id = id;
		this.username = username;
		this.client = client;
		this.dataNode = dataNode;
		action = action.trim().toUpperCase();
		if(action.equals("GET_FILES_BY_USER")) {
			this.action = FSAction.GET_FILES_BY_USER;
		} else if(action.equals("RETRIEVE")) {
			this.action = FSAction.RETRIEVE;
		} else if(action.equals("STORE")) {
			this.action = FSAction.STORE;
		} else if(action.equals("REMOVE")) {
			this.action = FSAction.REMOVE;
		}
	}
	
	public void run() {
	
		try {
		
			// Perform action
			if(action == FSAction.GET_FILES_BY_USER) {
				
				System.out.println(username + ": action GET_FILES_BY_USER");
			
				// Get file name list from data node
				String[] fileNames = MasterServer.getFilesByUsername(dataNode, username);
				
				// Send files to client
				LionFSUtil.writeInt(client, fileNames.length);
				for(String f : fileNames) {
					LionFSUtil.writeString(client, f);
				}
			} else if(action == FSAction.STORE) {
				
				System.out.println(username + ": action STORE");
					
				// Read file count
				int fileCount = LionFSUtil.readInt(client);
				
				// Read files
				List<FSFile> files = new ArrayList<>();
				for(int i = 0; i < fileCount; i++) {
					String name = LionFSUtil.readString(client);
					int size = LionFSUtil.readInt(client);
					byte[] fileb = LionFSUtil.readBytes(client, size);
					files.add(new FSFile(name, size, fileb));
				}
				
				// Write files
				MasterServer.storeFiles(dataNode, files, username);
				
				dataNode.setFileCount(dataNode.getFileCount() + files.size());
				
			} else if(action == FSAction.RETRIEVE) {
				
				System.out.println(username + ": action RETRIEVE");
	
				// Read file count
				int fileCount = LionFSUtil.readInt(client);
				
				// Read file names
				List<String> fileNames = new ArrayList<>();
				for(int i = 0; i < fileCount; i++) {
					String fileName = LionFSUtil.readString(client);
					fileNames.add(fileName);
				}
				
				// Get files from data node
				List<FSFile> files = MasterServer.retrieveFiles(dataNode, username, fileNames);
				
				// Write files
				for(int i = 0; i < files.size(); i++) {
					FSFile file = files.get(i);
					LionFSUtil.writeString(client, file.getName());
					LionFSUtil.writeInt(client, file.getSize());
					LionFSUtil.writeBytes(client, file.getData());
				}
				
			} else if(action == FSAction.REMOVE) {
				
				System.out.println(username + ": action REMOVE");
	
				// Read file name
				String fileName = LionFSUtil.readString(client);
				
				MasterServer.removeFile(dataNode, username, fileName);
				
				dataNode.setFileCount(dataNode.getFileCount() - 1);
				
			}
			
		} catch(IOException e) {
			e.printStackTrace();
		}
	}
	
}

