package com.course.kafka.broker.message;

import java.time.OffsetDateTime;

import com.fasterxml.jackson.annotation.JsonFormat;

public class CustomerPreferenceShoppingCartMessage {

	private String customerId;
	private String itemName;
	private int cartAmount;

	@JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSZ")
	private OffsetDateTime cartDatetime;

	public CustomerPreferenceShoppingCartMessage() {
	}

	public CustomerPreferenceShoppingCartMessage(String customerId, String itemName, int cartAmount,
			OffsetDateTime cartDatetime) {
		super();
		this.customerId = customerId;
		this.itemName = itemName;
		this.cartAmount = cartAmount;
		this.cartDatetime = cartDatetime;
	}

	public int getCartAmount() {
		return cartAmount;
	}

	public OffsetDateTime getCartDatetime() {
		return cartDatetime;
	}

	public String getCustomerId() {
		return customerId;
	}

	public String getItemName() {
		return itemName;
	}

	public void setCartAmount(int cartAmount) {
		this.cartAmount = cartAmount;
	}

	public void setCartDatetime(OffsetDateTime cartDatetime) {
		this.cartDatetime = cartDatetime;
	}

	public void setCustomerId(String customerId) {
		this.customerId = customerId;
	}

	public void setItemName(String itemName) {
		this.itemName = itemName;
	}

	@Override
	public String toString() {
		return "CustomerPreferenceShoppingCartMessage [customerId=" + customerId + ", itemName=" + itemName
				+ ", cartAmount=" + cartAmount + ", cartDatetime=" + cartDatetime + "]";
	}

}
