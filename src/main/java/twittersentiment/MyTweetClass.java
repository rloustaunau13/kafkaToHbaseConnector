package twittersentiment;

import java.io.Serializable;
import java.util.HashMap;

/**
 *
 * @author kamal
 * Wraps around HashMap to set and get tweet.  Serializer and Deserializer use this
 * class to receive and send bytes.
 */
public class MyTweetClass implements  Serializable {

    HashMap<String, String> tweet;

    public MyTweetClass() {
        this.tweet = new HashMap();
    }

    @Override
    public String toString() {
        return "MyTweetClass{" + "tweet=" + tweet + '}';
    }

    public HashMap getTweet() {
        return tweet;
    }

    public void setTweet(HashMap tweet) {
        this.tweet = tweet;
    }


}