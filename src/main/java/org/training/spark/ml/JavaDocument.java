package org.training.spark.ml;

import java.io.Serializable;

/**
 * Unlabeled instance type, Spark SQL can infer schema from Java Beans.
 */
@SuppressWarnings("serial")
public class JavaDocument implements Serializable {

  private long id;
  private String text;

  public JavaDocument(long id, String text) {
    this.id = id;
    this.text = text;
  }

  public long getId() {
    return this.id;
  }

  public String getText() {
    return this.text;
  }
}
