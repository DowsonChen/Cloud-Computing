import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.MultiMap;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;

import java.util.HashMap;
import java.util.Map;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

public class VerFrontEnd extends AbstractVerticle {

  private Map<String, JsonObject> products = new HashMap<>();
  
  static private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  // Set time zone at compile time
  static {
      dateFormat.setTimeZone(TimeZone.getTimeZone("GMT-5"));
  }

  static private Date date = new Date();

  // Get GCD method
  static private long getGCD(String key) {
      // create 3 BigInteger objects
      BigInteger bi1 = new BigInteger(key);
      BigInteger bi2 = new BigInteger(
        "64266330917908644872330635228106713310880186591609208114244758680898150367880703152525200743234420230");
      BigInteger bi3;
      // assign gcd of bi1, bi2 to bi3
      bi3 = bi1.gcd(bi2);
      return bi3.longValue();
  }

  static private String decipher(String message, String key) {
      // get the row and col number
      int len = message.length();
      int num = (int) Math.sqrt(len);
      long z = getGCD(key);
      // get intermediate key z
      int keyZ = (int) (1 + z % 25);
      // create a 2D matrix of message, and chage to original one
      char[][] cipherMatrix = new char[num][num];
      for (int i = 0; i < num; i++) {
          for (int j = 0; j < num; j++) {
              cipherMatrix[i][j] = message.charAt(i * num + j);
              int temp = cipherMatrix[i][j] - keyZ;
              // Assign value in 26 letters' loop
              cipherMatrix[i][j] = (char)( temp < 65 ? temp + 26 : temp);
          }
      }
      
      ArrayList<Character> list = new ArrayList<Character>();
      int time = 0;
      // Read data from spiral matrix
      while(time * 2 < num && time * 2 < num) {
          for(int i = time; i < num-time; i++) {
              list.add(cipherMatrix[time][i]);    
          }
          for(int i = time+1; i< num-time; i++) {
              list.add(cipherMatrix[i][num-time-1]);              
          }
          // if only one row /col remains
          if(num - 2 * time == 1 || num - 2 * time == 1) {
              break;  
          }
          for(int i = num-time-2; i>=time; i--) {
              list.add(cipherMatrix[num-time-1][i]);  
          }
          for(int i = num-time-2; i>= time+1; i--) {
              list.add(cipherMatrix[i][time]);    
          }
          time++;
      }

      StringBuilder builder = new StringBuilder(list.size());
      for (Character ch : list) {
          builder.append(ch);
      }
      return builder.toString();
    }




  @Override
  public void start() {

    Router router = Router.router(vertx);
    // Process q1
    router.getWithRegex("/q1.*").handler(this::handlePara);
    vertx.createHttpServer().requestHandler(router::accept).listen(80);
  }

  private void handlePara(RoutingContext routingContext) {
    HttpServerRequest request = routingContext.request();
    HttpServerResponse response = routingContext.response();
    MultiMap parameters = request.params();
    String key = parameters.get("key");
    String message = parameters.get("message");
    String result = decipher(message, key);
    String re = "OnePiece,691851324638\n" + dateFormat.format(date) + "\n" + result + "\n";
    response.end(re);
  }
}

