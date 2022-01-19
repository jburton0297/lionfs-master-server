
public class FSFile {

	private String name;
	private int size;
	private byte[] data;
	
	public FSFile(String name, int size, byte[] data) {
		this.name = name;
		this.size = size;
		this.data = data;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getSize() {
		return size;
	}

	public void setSize(int size) {
		this.size = size;
	}

	public byte[] getData() {
		return data;
	}

	public void setData(byte[] data) {
		this.data = data;
	}
	
}
