package ru.sut.fuzzybalancer;

public class LinkCostInfo {

	 protected long bytesTransferred = 0;
	 protected long bytesDelta = 0; 	 
	 protected int cost = 1;
		
	public long getBytesTransferred() {
		return bytesTransferred;
	}

	public void setBytesTransferred(long bytesTransferred) {
		this.bytesTransferred = bytesTransferred;
	}

	public long getBytesDelta() {
		return bytesDelta;
	}

	public void setBytesDelta(long bytesDelta) {
		this.bytesDelta = bytesDelta;
	}

	public int getCost() {
		return cost;
	}

	public void setCost(int cost) {
		this.cost = cost;
	}

	public void updateBytesTransferred(long bytesTransferred) {
		if (this.bytesTransferred != 0)
			bytesDelta = bytesTransferred - this.bytesTransferred;
		this.bytesTransferred = bytesTransferred;
	}

	 
	 
}
