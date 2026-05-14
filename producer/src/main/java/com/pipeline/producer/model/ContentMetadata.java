package com.pipeline.producer.model;

public class ContentMetadata {
    private String content_id;
    private String category;
    private String creator_id;
    private String publish_timestamp;

    public ContentMetadata() {}

    public ContentMetadata(String content_id, String category,
                           String creator_id, String publish_timestamp) {
        this.content_id        = content_id;
        this.category          = category;
        this.creator_id        = creator_id;
        this.publish_timestamp = publish_timestamp;
    }

    public String getContent_id()        { return content_id; }
    public void   setContent_id(String v)        { this.content_id = v; }
    public String getCategory()          { return category; }
    public void   setCategory(String v)          { this.category = v; }
    public String getCreator_id()        { return creator_id; }
    public void   setCreator_id(String v)        { this.creator_id = v; }
    public String getPublish_timestamp() { return publish_timestamp; }
    public void   setPublish_timestamp(String v) { this.publish_timestamp = v; }
}
