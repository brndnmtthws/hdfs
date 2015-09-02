package org.apache.mesos.hdfs.scheduler;

import java.util.List;
import java.util.Map;

import org.apache.mesos.Protos.Attribute;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.Value.Range;

/**
 * HDFS Mesos offer constraints checker class implementation.
 */
public class HdfsMesosConstraints {

  private final Map<String, String> constraints;

  public HdfsMesosConstraints(Map<String, String> mesosSlaveConstraints) {
    this.constraints = mesosSlaveConstraints;
  }

  public boolean constraintsAllow(Offer offer) {
    List<Attribute> attributes = offer.getAttributesList();

    for (Map.Entry<String, String> constraintEntry : this.constraints
        .entrySet()) {
      boolean found = false;
      String constraintName = constraintEntry.getKey();
      String constraintValue = constraintEntry.getValue();
      for (Attribute attribute : attributes) {

        if (attribute.getName().equals(constraintName)) {

          switch (attribute.getType()) {
          case RANGES:
            if (attribute.hasRanges()) {
              try {
                Long value = Long.parseLong(constraintValue);
                for (Range r : attribute.getRanges().getRangeList()) {
                  if ((!r.hasBegin() || value >= r.getBegin())
                      && (!r.hasEnd() || value <= r.getEnd())) {
                    found = true;
                    break;
                  }
                }
              } catch (NumberFormatException e) {
                found = false;
                // not a range attribute
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
                found = false;
                // not a scalar attribute
              }
            }
            break;
          case SET:
            if (attribute.hasSet()) {
              boolean isSubset = true;
              for (String element : constraintValue.split("=")) {
                if (!attribute.getSet().getItemList().contains(element)) {
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
