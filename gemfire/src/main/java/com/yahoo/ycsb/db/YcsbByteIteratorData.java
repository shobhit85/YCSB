/**
 * 
 */
package com.yahoo.ycsb.db;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.gemstone.gemfire.pdx.PdxReader;
import com.gemstone.gemfire.pdx.PdxSerializable;
import com.gemstone.gemfire.pdx.PdxWriter;
import com.yahoo.ycsb.ByteIterator;

/**
 * @author shobhit
 *
 */
public class YcsbByteIteratorData implements PdxSerializable {

  private List<String> fields;
  private List<String> fieldValues;
  private int size;

  public YcsbByteIteratorData(Map<String, String> values) {
    fields = new ArrayList<String>(values.keySet());
    fieldValues = new ArrayList<String>(values.values());
    size = values.size();
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.pdx.PdxSerializable#fromData(com.gemstone.gemfire.pdx.PdxReader)
   */
  @Override
  public void fromData(PdxReader rdr) {
    size = rdr.readInt("size");
    throw new RuntimeException("Unexpected deserialization of YcsbByteIteratorData");    
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.pdx.PdxSerializable#toData(com.gemstone.gemfire.pdx.PdxWriter)
   */
  @Override
  public void toData(PdxWriter wtr) {
    wtr.writeInt("size", size);
    for (int index=0; index<size; index++) {
      String field = fields.get(index);
      String fieldValue = fieldValues.get(index);
      wtr.writeString(field, fieldValue);
    }
  }
}
