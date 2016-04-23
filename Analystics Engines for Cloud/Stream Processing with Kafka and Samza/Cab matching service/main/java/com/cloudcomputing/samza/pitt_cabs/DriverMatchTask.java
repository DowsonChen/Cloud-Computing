package com.cloudcomputing.samza.pitt_cabs;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.io.IOException;
import java.util.HashMap;
import java.util.NoSuchElementException;

import org.apache.samza.config.Config;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.WindowableTask;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;

/**
 * Consumes the stream of driver location updates and rider cab requests.
 * Outputs a stream which joins these 2 streams and gives a stream of rider
 * to driver matches.
 */
public class DriverMatchTask implements StreamTask, InitableTask {

  /* Define per task state here. (kv stores etc) */
  private KeyValueStore<String, String> driverLoc;
  private HashMap<String, Queue<Integer>> map = new HashMap<String, Queue<Integer>>();

  @Override
  @SuppressWarnings("unchecked")
  public void init(Config config, TaskContext context) throws Exception {
	//Initialize stuff (maybe the kv stores?)
    driverLoc = (KeyValueStore<String, String>)context.getStore("driver-loc");
  }

  @Override
  @SuppressWarnings("unchecked")
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
	// The main part of your code. Remember that all the messages for a particular partition
	// come here (somewhat like MapReduce). So for task 1 messages for a blockId will arrive
	// at one task only, thereby enabling you to do stateful stream processing.
    String incomingStream = envelope.getSystemStreamPartition().getStream();
    // Check the incoming stream is driver location or event
    if (incomingStream.equals(DriverMatchConfig.DRIVER_LOC_STREAM.getStream())) {
      // Process driver location stream
      processDriverLocStream((Map<String, Object>) envelope.getMessage(), collector);
    } else if (incomingStream.equals(DriverMatchConfig.EVENT_STREAM.getStream())) {
      // Process event stream
      processEventStream((Map<String, Object>) envelope.getMessage(), collector);
    } else {
      throw new IllegalStateException("Unexpected input stream: " + envelope.getSystemStreamPartition());
    }
  }

  private void processDriverLocStream(Map<String, Object> message, MessageCollector collector) {
    if (!message.get("type").equals("DRIVER_LOCATION")) {
      throw new IllegalStateException("Unexpected event type on follows stream: " + message.get("event"));
    }
    // Get values from stream
    String blockId = String.valueOf((int) message.get("blockId"));
    String driverId = String.valueOf((int) message.get("driverId"));
    String latitude = String.valueOf((int) message.get("latitude"));
    String longitude = String.valueOf((int) message.get("longitude"));
    // Put in kv store, key : value
    driverLoc.put(blockId + ":" + driverId, longitude + ":" + latitude);
  }

  private void processEventStream(Map<String, Object> message, MessageCollector collector) {
    if (message.get("type").equals("RIDE_COMPLETE")) {
      String blockId = String.valueOf((int) message.get("blockId"));
      String driverId = String.valueOf((int) message.get("driverId"));
      String latitude = String.valueOf((int) message.get("latitude"));
      String longitude = String.valueOf((int) message.get("longitude"));
      String gender = (String) message.get("gender");
      String rating = String.valueOf((double) message.get("rating"));
      String salary = String.valueOf((int) message.get("salary"));
      // If order complete, put the driver back to kv store
      driverLoc.put(blockId + ":" + driverId, longitude + ":" + latitude + ":" + gender + ":" + rating + ":" + salary);

    } else if (message.get("type").equals("ENTERING_BLOCK")) {
      String blockId = String.valueOf((int) message.get("blockId"));
      String driverId = String.valueOf((int) message.get("driverId"));
      String latitude = String.valueOf((int) message.get("latitude"));
      String longitude = String.valueOf((int) message.get("longitude"));
      String gender = (String) message.get("gender");
      String rating = String.valueOf((double) message.get("rating"));
      String salary = String.valueOf((int) message.get("salary"));
      if (message.get("status").equals("UNAVAILABLE")) {
        driverLoc.delete(blockId + ":" + driverId);
      } else {
        driverLoc.put(blockId + ":" + driverId, longitude + ":" + latitude + ":" + gender + ":" + rating + ":" + salary);
      }

    } else if (message.get("type").equals("LEAVING_BLOCK")) {
      // Get all information and save
      String blockId = String.valueOf((int) message.get("blockId"));
      String driverId = String.valueOf((int) message.get("driverId"));
      driverLoc.delete(blockId + ":" + driverId);

    } else {
      String clientId = String.valueOf((int) message.get("clientId"));
      String clientBlockId = String.valueOf((int) message.get("blockId"));
      int clientLongitude = (int) message.get("longitude");
      int clientLatitude =(int) message.get("latitude");
      String clientPrefer = (String) message.get("gender_preference");
      KeyValueIterator<String, String> drivers = driverLoc.range(clientBlockId + ":", clientBlockId + ";");
      String driverId = null;
      String matchKey = null;
      double priceFactor = 0.0;
      int driverRatio = 0;
      double MAX_DIST = 500 * Math.sqrt(2);
      // double MAX_DIST = 500;
      double maxScore = Double.MIN_VALUE;
      try {
        while (drivers.hasNext()) {
          driverRatio++;
          Entry<String, String> entry = drivers.next();
          String currKey = entry.getKey();
          String currVal = entry.getValue();
          String[] driverInfo = currVal.split(":");
          if (driverInfo.length == 5) {
            int driverLongtitude = Integer.parseInt(driverInfo[0]);
            int driverLatitude = Integer.parseInt(driverInfo[1]);
            String gender = driverInfo[2];
            double rating = Double.parseDouble(driverInfo[3]);
            int salary = Integer.parseInt(driverInfo[4]);
            // Calculate score
            double distance_score = 1 - Math.sqrt((Math.pow((clientLongitude - driverLongtitude), 2) 
              + Math.pow((clientLatitude - driverLatitude), 2))) * 1.0 / MAX_DIST;
            double gender_score = 0.0;
            if (gender.equals(clientPrefer) || clientPrefer.equals("N")) {
              gender_score = 1.0;
            }
            double rating_score = rating / 5.0;
            double salary_score = 1 - salary / 100.0;
            double match_score = distance_score * 0.4 + gender_score * 0.2 + rating_score * 0.2 + salary_score * 0.2;
            if (match_score > maxScore){
              maxScore = match_score;
              matchKey = currKey;
              driverId = currKey.split(":")[1];
            }
          }
          
          if (!map.containsKey(clientBlockId)) {
              Queue<Integer> queue = new LinkedList<Integer>();
              queue.offer(driverRatio);
              map.put(clientBlockId, queue);
          } else {
              map.get(clientBlockId).offer(driverRatio);
          } 
          
          if (map.get(clientBlockId).size() < 5) {
              priceFactor = 1.0;
          } else {
              Queue<Integer> temp = map.get(clientBlockId); 
              if (temp.size() > 5) {
                  temp.poll();
              }
              int sum = 0;
              for (int i = 1; i <= 5; i++) {
                  int curr = temp.poll();
                  sum += curr;
                  temp.offer(curr);
              }
              double average = sum * 1.0 / 5;
              if (average >= 3.6) {
                  priceFactor = 1.0; 
              } else {
                  priceFactor =  (4 * (3.6 - average) / (1.8 - 1)) + 1.0;
              }
          }

        }
      } catch (NoSuchElementException e) {
        System.out.println("Error happens inside even stream");
      } finally {
        drivers.close();
      }

      HashMap<String, Object> result = new HashMap<>();
      if ((driverId != null) && (matchKey != null) && (priceFactor != 0.0)) {
          result.put("clientId", clientId);
          result.put("driverId", driverId);
          result.put("priceFactor", String.valueOf(priceFactor));
          driverLoc.delete(matchKey);
          collector.send(new OutgoingMessageEnvelope(DriverMatchConfig.MATCH_STREAM, result));
      }
    }
  }

}
