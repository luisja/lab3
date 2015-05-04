package es.unizar.tmdad.lab3.domain;

import java.util.Date;
import java.util.Map;

import org.springframework.social.twitter.api.Entities;
import org.springframework.social.twitter.api.Tweet;
import org.springframework.social.twitter.api.TwitterProfile;

public class MyTweet {

	private Tweet tweet;
	
	private String text;

	public MyTweet(Tweet tweet) {
		this.tweet = tweet;
	}
	
	public MyTweet(String text) {
		this.text = text;
	}

	public Map<String, Object> getExtraData() {
		return tweet.getExtraData();
	}

	public String getText() {
		return tweet.getText();
	}
	
	public void setUnmodifiedText(String text) {
		this.text = text;
	}

	public String getUnmodifiedText() {
		return text!=null? text: tweet.getUnmodifiedText();
	}

	public Date getCreatedAt() {
		return tweet.getCreatedAt();
	}

	public String getFromUser() {
		return tweet.getFromUser();
	}

	public void setFromUser(String fromUser) {
		tweet.setFromUser(fromUser);
	}

	public long getId() {
		return tweet.getId();
	}

	public String getProfileImageUrl() {
		return tweet.getProfileImageUrl();
	}

	public void setProfileImageUrl(String profileImageUrl) {
		tweet.setProfileImageUrl(profileImageUrl);
	}

	public Long getToUserId() {
		return tweet.getToUserId();
	}

	public void setToUserId(Long toUserId) {
		tweet.setToUserId(toUserId);
	}

	public long getFromUserId() {
		return tweet.getFromUserId();
	}

	public void setInReplyToStatusId(Long inReplyToStatusId) {
		tweet.setInReplyToStatusId(inReplyToStatusId);
	}

	public Long getInReplyToStatusId() {
		return tweet.getInReplyToStatusId();
	}

	public void setFromUserId(long fromUserId) {
		tweet.setFromUserId(fromUserId);
	}

	public String getLanguageCode() {
		return tweet.getLanguageCode();
	}

	public void setLanguageCode(String languageCode) {
		tweet.setLanguageCode(languageCode);
	}

	public String getSource() {
		return tweet.getSource();
	}

	public void setSource(String source) {
		tweet.setSource(source);
	}

	public void setRetweetCount(Integer retweetCount) {
		tweet.setRetweetCount(retweetCount);
	}

	public Integer getRetweetCount() {
		return tweet.getRetweetCount();
	}

	public void setRetweeted(boolean retweeted) {
		tweet.setRetweeted(retweeted);
	}

	public boolean isRetweeted() {
		return tweet.isRetweeted();
	}

	public Tweet getRetweetedStatus() {
		return tweet.getRetweetedStatus();
	}

	public void setRetweetedStatus(Tweet tweet) {
		tweet.setRetweetedStatus(tweet);
	}

	public boolean isRetweet() {
		return tweet.isRetweet();
	}

	public void setFavorited(boolean favorited) {
		tweet.setFavorited(favorited);
	}

	public boolean isFavorited() {
		return tweet.isFavorited();
	}

	public void setFavoriteCount(Integer favoriteCount) {
		tweet.setFavoriteCount(favoriteCount);
	}

	public Integer getFavoriteCount() {
		return tweet.getFavoriteCount();
	}

	public Entities getEntities() {
		return tweet.getEntities();
	}

	public void setEntities(Entities ent) {
		tweet.setEntities(ent);
	}

	public boolean hasMentions() {
		return tweet.hasMentions();
	}

	public boolean hasMedia() {
		return tweet.hasMedia();
	}

	public boolean hasUrls() {
		return tweet.hasUrls();
	}

	public boolean hasTags() {
		return tweet.hasTags();
	}

	public TwitterProfile getUser() {
		return tweet.getUser();
	}

	public void setUser(TwitterProfile prof) {
		tweet.setUser(prof);
	}

	public Long getInReplyToUserId() {
		return tweet.getInReplyToUserId();
	}

	public void setInReplyToUserId(Long inReplyToUserId) {
		tweet.setInReplyToUserId(inReplyToUserId);
	}

	public String getInReplyToScreenName() {
		return tweet.getInReplyToScreenName();
	}

	public void setInReplyToScreenName(String inReplyToScreenName) {
		tweet.setInReplyToScreenName(inReplyToScreenName);
	}

	@Override
	public boolean equals(Object o) {
		return tweet.equals(o);
	}

	@Override
	public int hashCode() {
		return tweet.hashCode();
	}

	@Override
	public String toString() {
		return tweet.toString();
	}
	
}
