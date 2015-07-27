/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
@SuppressWarnings("all")
/** A pair of strings with an added field. */
@org.apache.avro.specific.AvroGenerated
public class StringPair extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"StringPair\",\"doc\":\"A pair of strings with an added field.\",\"fields\":[{\"name\":\"left\",\"type\":\"string\"},{\"name\":\"right\",\"type\":\"string\"},{\"name\":\"description\",\"type\":\"string\",\"default\":\"\"}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  @Deprecated public java.lang.CharSequence left;
  @Deprecated public java.lang.CharSequence right;
  @Deprecated public java.lang.CharSequence description;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>. 
   */
  public StringPair() {}

  /**
   * All-args constructor.
   */
  public StringPair(java.lang.CharSequence left, java.lang.CharSequence right, java.lang.CharSequence description) {
    this.left = left;
    this.right = right;
    this.description = description;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return left;
    case 1: return right;
    case 2: return description;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: left = (java.lang.CharSequence)value$; break;
    case 1: right = (java.lang.CharSequence)value$; break;
    case 2: description = (java.lang.CharSequence)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'left' field.
   */
  public java.lang.CharSequence getLeft() {
    return left;
  }

  /**
   * Sets the value of the 'left' field.
   * @param value the value to set.
   */
  public void setLeft(java.lang.CharSequence value) {
    this.left = value;
  }

  /**
   * Gets the value of the 'right' field.
   */
  public java.lang.CharSequence getRight() {
    return right;
  }

  /**
   * Sets the value of the 'right' field.
   * @param value the value to set.
   */
  public void setRight(java.lang.CharSequence value) {
    this.right = value;
  }

  /**
   * Gets the value of the 'description' field.
   */
  public java.lang.CharSequence getDescription() {
    return description;
  }

  /**
   * Sets the value of the 'description' field.
   * @param value the value to set.
   */
  public void setDescription(java.lang.CharSequence value) {
    this.description = value;
  }

  /** Creates a new StringPair RecordBuilder */
  public static StringPair.Builder newBuilder() {
    return new StringPair.Builder();
  }
  
  /** Creates a new StringPair RecordBuilder by copying an existing Builder */
  public static StringPair.Builder newBuilder(StringPair.Builder other) {
    return new StringPair.Builder(other);
  }
  
  /** Creates a new StringPair RecordBuilder by copying an existing StringPair instance */
  public static StringPair.Builder newBuilder(StringPair other) {
    return new StringPair.Builder(other);
  }
  
  /**
   * RecordBuilder for StringPair instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<StringPair>
    implements org.apache.avro.data.RecordBuilder<StringPair> {

    private java.lang.CharSequence left;
    private java.lang.CharSequence right;
    private java.lang.CharSequence description;

    /** Creates a new Builder */
    private Builder() {
      super(StringPair.SCHEMA$);
    }
    
    /** Creates a Builder by copying an existing Builder */
    private Builder(StringPair.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.left)) {
        this.left = data().deepCopy(fields()[0].schema(), other.left);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.right)) {
        this.right = data().deepCopy(fields()[1].schema(), other.right);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.description)) {
        this.description = data().deepCopy(fields()[2].schema(), other.description);
        fieldSetFlags()[2] = true;
      }
    }
    
    /** Creates a Builder by copying an existing StringPair instance */
    private Builder(StringPair other) {
            super(StringPair.SCHEMA$);
      if (isValidValue(fields()[0], other.left)) {
        this.left = data().deepCopy(fields()[0].schema(), other.left);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.right)) {
        this.right = data().deepCopy(fields()[1].schema(), other.right);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.description)) {
        this.description = data().deepCopy(fields()[2].schema(), other.description);
        fieldSetFlags()[2] = true;
      }
    }

    /** Gets the value of the 'left' field */
    public java.lang.CharSequence getLeft() {
      return left;
    }
    
    /** Sets the value of the 'left' field */
    public StringPair.Builder setLeft(java.lang.CharSequence value) {
      validate(fields()[0], value);
      this.left = value;
      fieldSetFlags()[0] = true;
      return this; 
    }
    
    /** Checks whether the 'left' field has been set */
    public boolean hasLeft() {
      return fieldSetFlags()[0];
    }
    
    /** Clears the value of the 'left' field */
    public StringPair.Builder clearLeft() {
      left = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /** Gets the value of the 'right' field */
    public java.lang.CharSequence getRight() {
      return right;
    }
    
    /** Sets the value of the 'right' field */
    public StringPair.Builder setRight(java.lang.CharSequence value) {
      validate(fields()[1], value);
      this.right = value;
      fieldSetFlags()[1] = true;
      return this; 
    }
    
    /** Checks whether the 'right' field has been set */
    public boolean hasRight() {
      return fieldSetFlags()[1];
    }
    
    /** Clears the value of the 'right' field */
    public StringPair.Builder clearRight() {
      right = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    /** Gets the value of the 'description' field */
    public java.lang.CharSequence getDescription() {
      return description;
    }
    
    /** Sets the value of the 'description' field */
    public StringPair.Builder setDescription(java.lang.CharSequence value) {
      validate(fields()[2], value);
      this.description = value;
      fieldSetFlags()[2] = true;
      return this; 
    }
    
    /** Checks whether the 'description' field has been set */
    public boolean hasDescription() {
      return fieldSetFlags()[2];
    }
    
    /** Clears the value of the 'description' field */
    public StringPair.Builder clearDescription() {
      description = null;
      fieldSetFlags()[2] = false;
      return this;
    }

    @Override
    public StringPair build() {
      try {
        StringPair record = new StringPair();
        record.left = fieldSetFlags()[0] ? this.left : (java.lang.CharSequence) defaultValue(fields()[0]);
        record.right = fieldSetFlags()[1] ? this.right : (java.lang.CharSequence) defaultValue(fields()[1]);
        record.description = fieldSetFlags()[2] ? this.description : (java.lang.CharSequence) defaultValue(fields()[2]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }
}
