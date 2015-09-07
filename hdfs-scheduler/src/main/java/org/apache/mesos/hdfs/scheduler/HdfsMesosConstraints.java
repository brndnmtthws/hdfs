package org.apache.mesos.hdfs.scheduler;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.mesos.Protos.Attribute;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.Value.Range;
import org.apache.mesos.hdfs.config.HdfsFrameworkConfig;

/**
 * HDFS Mesos offer constraints checker class implementation.
 */
public class HdfsMesosConstraints {

  private final HdfsFrameworkConfig config;

  public HdfsMesosConstraints(HdfsFrameworkConfig config) {
    this.config = config;
  }

  public boolean constraintsAllow(Offer offer) {
    List<Attribute> attributes = offer.getAttributesList();
    
    Map<String, String> constraints = config.getMesosSlaveConstraints();
    Set<Map.Entry<String, String>> constraintSet = constraints.entrySet();
    
    for (Map.Entry<String, String> constraintEntry : constraintSet) {
      boolean found = false;
      String constraintName = constraintEntry.getKey();
      String constraintValue = constraintEntry.getValue();
      
      for (Attribute attribute : attributes) {
        if (attribute.getName().equals(constraintName)) {
          switch (attribute.getType()) {
          case RANGES:
            if (attribute.hasRanges()) {
              try {
                Long range = Long.parseLong(constraintValue);
                for (Range r : attribute.getRanges().getRangeList()) {
                  if ((!r.hasBegin() || range >= r.getBegin())
                      && (!r.hasEnd() || range <= r.getEnd())) {
                    found = true;
                    break;
                  }
                }
              } catch (NumberFormatException e) {
                // Offer attribute value is not castble to number. Found is already false.
                // This is not error. It was just a trial.
                // Setting it false explicitly again to avoid empty catch build error
                found = false;
              }
            } 
            break;
          case SCALAR:
            if (attribute.hasScalar()) {
              try {
                if (attribute.getScalar().getValue() >= Double
                    .parseDouble(constraintValue)) {
                  found = true;
                }
              } catch (NumberFormatException e) {                
                // Offer attribute value is not castble to scalar. Found is already false.
                // This is not error. It was just a trial.
                // Setting it false explicitly again to avoid empty catch build error
                found = false;
              }
            }
            break;
          case SET:
            if (attribute.hasSet()) {
              boolean isSubset = true;
              List<String> attributeSetValues = attribute.getSet().getItemList();
              String[] constraintSetValues = constraintValue.split(",");
              for (String element : constraintSetValues) {
                if (!attributeSetValues.contains(element)) {
                  isSubset = false;
                  break;
                }
              }
              found = isSubset;
            }
            break;
          case TEXT:
            if (attribute.hasText()
                && (!attribute.getText().hasValue() || attribute.getText()
                    .getValue().equals(constraintValue))) {
              found = true;
              break;
            }
            break;
          default:
            break;
          }
        }

        if (found) {
          break;
        }
      }

      if (!found) {
        return false;
      }
    }

    return true;
  }
}
