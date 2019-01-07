package kafkahbaseconnect;

import java.io.Serializable;
import java.util.HashMap;

public class MyTweetClass implements Serializable {


    HashMap<String,String> tweet;


    public MyTweetClass() {

        this.tweet =new HashMap();

    }



    @Override
    public String toString(){
        return "kafkahbaseconnect.MyTweetClass{"+"tweet="+tweet +'}';
    }

    public HashMap getTweet(){
        return tweet;
    }


    public void setTweet(HashMap tweet){

        this.tweet=tweet;
    }

}
