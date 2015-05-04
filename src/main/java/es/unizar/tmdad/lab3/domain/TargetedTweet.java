package es.unizar.tmdad.lab3.domain;

import java.util.Arrays;
import java.util.List;

public class TargetedTweet {
	
	private MyTweet tweet;
	
	private List<String> targets;

	public TargetedTweet(MyTweet tweet, List<String> targets) {
		this.tweet = tweet;
		this.targets = targets;
	}
	
	public TargetedTweet(MyTweet tweet, String target) {
		this.tweet = tweet;
		this.targets = Arrays.asList(target);
	}

	public MyTweet getTweet() {
		return tweet;
	}
	
	public List<String> getTargets() {
		return targets;
	}

	public String getFirstTarget() {
		return targets.get(0);
	}

}
