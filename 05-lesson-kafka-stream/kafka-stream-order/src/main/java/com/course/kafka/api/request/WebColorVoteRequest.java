package com.course.kafka.api.request;

import java.time.OffsetDateTime;

import com.fasterxml.jackson.annotation.JsonFormat;

public class WebColorVoteRequest {

	private String color;

	private String username;

	@JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSZ")
	private OffsetDateTime voteDateTime;

	public String getColor() {
		return color;
	}

	public String getUsername() {
		return username;
	}

	public OffsetDateTime getVoteDateTime() {
		return voteDateTime;
	}

	public void setColor(String color) {
		this.color = color;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public void setVoteDateTime(OffsetDateTime voteDateTime) {
		this.voteDateTime = voteDateTime;
	}

	@Override
	public String toString() {
		return "WebColorVoteRequest [color=" + color + ", voteDateTime=" + voteDateTime + ", username=" + username
				+ "]";
	}

}
