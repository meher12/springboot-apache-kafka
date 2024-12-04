package com.course.kafka.broker.message;

import java.time.OffsetDateTime;

import com.fasterxml.jackson.annotation.JsonFormat;

public class CustomerPreferenceWishlistMessage {

	private String customerId;
	private String itemName;

	@JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSZ")
	private OffsetDateTime wishlistDatetime;

	public CustomerPreferenceWishlistMessage() {
	}

	public CustomerPreferenceWishlistMessage(String customerId, String itemName, OffsetDateTime wishlistDatetime) {
		super();
		this.customerId = customerId;
		this.itemName = itemName;
		this.wishlistDatetime = wishlistDatetime;
	}

	public String getCustomerId() {
		return customerId;
	}

	public String getItemName() {
		return itemName;
	}

	public OffsetDateTime getWishlistDatetime() {
		return wishlistDatetime;
	}

	public void setCustomerId(String customerId) {
		this.customerId = customerId;
	}

	public void setItemName(String itemName) {
		this.itemName = itemName;
	}

	public void setWishlistDatetime(OffsetDateTime wishlistDatetime) {
		this.wishlistDatetime = wishlistDatetime;
	}

}
