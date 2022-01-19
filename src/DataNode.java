
public class DataNode {

	public static final int MAX_USERS = 20;
	public static final int MAX_FILES = 1000;
	
	private String name;
	private String ip;
	private int port;
	
	private int userCount;
	private int fileCount;
	
	public DataNode(String name, String ip, int port) {
		this.name = name;
		this.ip = ip;
		this.port = port;
		this.userCount = 0;
		this.fileCount = 0;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public int getUserCount() {
		return userCount;
	}

	public void setUserCount(int userCount) {
		this.userCount = userCount;
	}

	public int getFileCount() {
		return fileCount;
	}

	public void setFileCount(int fileCount) {
		this.fileCount = fileCount;
	}
	
	public DataNode least(DataNode oNode) {
		if(this.userCount < oNode.getUserCount()) {
			return this;
		} else {
			return oNode;
		}
	}
	
}
