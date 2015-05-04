package es.unizar.tmdad.lab3.configuration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.social.twitter.api.impl.TwitterTemplate;
 
@Configuration
public class TwitterConfigurationTemplate {
    @Value("${twitter.consumerKey}")
    private String consumerKey;
 
    @Value("${twitter.consumerSecret}")
    private String consumerSecret;
 
    @Value("${twitter.accessToken}")
    private String accessToken;
 
    @Value("${twitter.accessTokenSecret}")
    private String accessTokenSecret;
 
    @Bean
    public TwitterTemplate twitterTemplate() {
        TwitterTemplate twitterOperations = new TwitterTemplate(consumerKey,
                consumerSecret, accessToken, accessTokenSecret);
        return twitterOperations;
    }
}