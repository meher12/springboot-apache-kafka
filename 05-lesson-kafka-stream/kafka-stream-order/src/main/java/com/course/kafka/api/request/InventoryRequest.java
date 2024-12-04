package com.course.kafka.api.request;

import java.time.OffsetDateTime;

import com.fasterxml.jackson.annotation.JsonFormat;

public class InventoryRequest {

	private String item;
	private long quantity;
	private String location;

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

	@Override
	public String toString() {
		return "InventoryRequest [item=" + item + ", quantity=" + quantity + ", location=" + location
				+ ", transactionTime=" + transactionTime + "]";
	}

}
