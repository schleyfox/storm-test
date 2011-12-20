package storm.test;

import backtype.storm.spout.ISpout;
import backtype.storm.utils.InprocMessaging;
import backtype.storm.utils.Utils;
import backtype.storm.task.TopologyContext;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.testing.FixedTuple;
import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.UUID;
import static backtype.storm.utils.Utils.get;

/**
 * A hybrid class living in a world between FixedTupleSpout and FeederSpout
 *
 * Used to mock out spouts in capturing-topology
 */
public class MultiStreamFeederSpout implements ISpout {
  private static Map<String, Integer> acked = new HashMap<String, Integer>();
  private static Map<String, Integer> failed = new HashMap<String, Integer>();
  private static Map<String, Integer> emitted = new HashMap<String, Integer>();
  private static Map<String, Integer> fed = new HashMap<String, Integer>();

  public static int getNumAcked(String stormId) {
    synchronized(acked) {
      return get(acked, stormId, 0);
    }
  }

  public static int getNumFailed(String stormId) {
    synchronized (acked) {
      return get(failed, stormId, 0);
    }
  }

  public static int getNumEmitted(String stormId) {
    synchronized (emitted) {
      return get(emitted, stormId, 0);
    }
  }

  public static int getNumFed(String stormId) {
    synchronized (fed) {
      return get(fed, stormId, 0);
    }
  }

  public static void clear(String stormId) {
    acked.remove(stormId);
    failed.remove(stormId);
    emitted.remove(stormId);
  }

  private int _id;
  private SpoutOutputCollector _collector;
  private TopologyContext _context;


  public MultiStreamFeederSpout() {
    _id = InprocMessaging.acquireNewPort();
  }

  public void feed(String stormId, List<Object> values) {
    FixedTuple tuple = new FixedTuple(values);
    feed(stormId, tuple);
  }

  public void feed(String stormId, String stream, List<Object> values) {
    FixedTuple tuple = new FixedTuple(stream, values);
    feed(stormId, tuple);
  }

  public void feed(String stormId, FixedTuple tuple) {
    synchronized(fed) {
      int curr = get(fed, stormId, 0);
      fed.put(stormId, curr+1);
    }
    InprocMessaging.sendMessage(_id, tuple);
  }

  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    _context = context;
    _collector = collector;
  }

  public void close() {
  }

  public void nextTuple() {
    FixedTuple tuple = null;
    tuple = (FixedTuple) InprocMessaging.pollMessage(_id);

    if(tuple!=null) {
      synchronized(emitted) {
        int curr = get(emitted, _context.getStormId(), 0);
        emitted.put(_context.getStormId(), curr+1);
      }
      _collector.emit(tuple.stream, tuple.values, UUID.randomUUID().toString());
    } else {
      Utils.sleep(10);
    }
  }

  public void ack(Object msgId) {
    synchronized(acked) {
      int curr = get(acked, _context.getStormId(), 0);
      acked.put(_context.getStormId(), curr+1);
    }
  }

  public void fail(Object msgId) {
    synchronized(failed) {
      int curr = get(failed, _context.getStormId(), 0);
      failed.put(_context.getStormId(), curr+1);
    }
  }
}
