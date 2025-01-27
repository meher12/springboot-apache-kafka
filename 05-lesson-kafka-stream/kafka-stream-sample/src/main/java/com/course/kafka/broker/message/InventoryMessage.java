package com.course.kafka.broker.message;

import java.time.OffsetDateTime;

import com.fasterxml.jackson.annotation.JsonFormat;

public class InventoryMessage {

	private String item;
	private String location;
	private long quantity;
	private String type;

	@JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSZ")
	private OffsetDateTime transactionTime;

	public String getItem() {
		return item;
	}

	public String getLocation() {
		return location;
	}

	public long getQuantity() {
		return quantity;
	}

	public OffsetDateTime getTransactionTime() {
		return transactionTime;
	}

	public String getType() {
		return type;
	}

	public void setItem(String item) {
		this.item = item;
	}

	public void setLocation(String location) {
		this.location = location;
	}

	public void setQuantity(long quantity) {
		this.quantity = quantity;
	}

	public void setTransactionTime(OffsetDateTime transactionTime) {
		this.transactionTime = transactionTime;
	}

	public void setType(String type) {
		this.type = type;
	}

	@Override
	public String toString() {
		return "InventoryMessage [item=" + item + ", location=" + location + ", quantity=" + quantity + ", type=" + type
				+ ", transactionTime=" + transactionTime + "]";
	}

}
