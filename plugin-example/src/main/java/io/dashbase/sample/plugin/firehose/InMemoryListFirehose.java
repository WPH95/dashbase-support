package io.dashbase.sample.plugin.firehose;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;

import rapid.firehose.RapidFirehose;
import rapid.firehose.RapidFirehoseMessage;
import rapid.server.config.Configurable;

public class InMemoryListFirehose extends RapidFirehose implements Configurable
{
  private static Logger logger = LoggerFactory.getLogger(InMemoryListFirehose.class);
  private int count = 0;
  private Iterator<byte[]> dataIter = null;

  @Override
  public void seekToOffset(String offset)
  {
    try {
      int offVal = Integer.parseInt(offset);
      count = offVal;
      int i = 0;
      while (i < count) {
        dataIter.next();
        i++;
      }
    } catch (Exception e) {
      logger.error("cannot seek to offset: " + offset, e);
    }
  }

  @Override
  public boolean isDrained()
  {
    return dataIter == null || !dataIter.hasNext();
  }

  public void setList(List<byte[]> dataList)
  {
    this.dataIter = dataList.iterator();
  }

  @Override
  public Iterator<RapidFirehoseMessage> iterator()
  {
    return new Iterator<RapidFirehoseMessage>()
    {

      @Override
      public boolean hasNext()
      {
        return dataIter.hasNext();
      }

      @Override
      public RapidFirehoseMessage next()
      {
        byte[] data = dataIter.next();
        RapidFirehoseMessage msg = new RapidFirehoseMessage()
        {

          @Override
          public String offset()
          {
            return String.valueOf(count);
          }

          @Override
          public byte[] data()
          {
            return data;
          }
        };
        count++;
        return msg;
      }

    };
  }

  @Override
  public void start() throws Exception
  {
  }

  @Override
  public void shutdown() throws Exception
  {
  }

  /**
   * "firehose": {
   *     "params": {
   *         "values": "one,two,three,four,five"
   *     },
   *     "clazz": "io.dashbase.sample.plugin.firehose.InMemoryListFirehose"
   * }
   */
  @Override
  public void configure(Map<String, Object> params)
  {
    if (params != null && params.containsKey("values")) {
      String val = (String) params.get("values");
      if (val != null) {
        String[] valList = val.split(",");
        List<byte[]> dataList = Lists.newArrayListWithCapacity(valList.length);
        for (String s : valList) {
          dataList.add(s.getBytes(Charsets.UTF_8));
        }
        setList(dataList);
      }
    }
    
  }
}
