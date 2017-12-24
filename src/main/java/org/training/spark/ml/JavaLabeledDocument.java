package org.training.spark.ml;

import java.io.Serializable;

/**
 * Labeled instance type, Spark SQL can infer schema from Java Beans.
 */
@SuppressWarnings("serial")
public class JavaLabeledDocument extends JavaDocument implements Serializable {

  private double label;

  public JavaLabeledDocument(long id, String text, double label) {
    super(id, text);
    this.label = label;
  }

  public double getLabel() {
    return this.label;
  }
}
