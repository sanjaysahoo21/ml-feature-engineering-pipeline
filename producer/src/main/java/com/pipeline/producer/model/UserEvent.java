package com.pipeline.producer.model;

public class UserEvent {
    private String user_id;
    private String content_id;
    private String event_type;
    private int    dwell_time_ms;
    private String timestamp;

    public UserEvent() {}

    public UserEvent(String user_id, String content_id, String event_type,
                     int dwell_time_ms, String timestamp) {
        this.user_id       = user_id;
        this.content_id    = content_id;
        this.event_type    = event_type;
        this.dwell_time_ms = dwell_time_ms;
        this.timestamp     = timestamp;
    }

    public String getUser_id()       { return user_id; }
    public void   setUser_id(String v)   { this.user_id = v; }
    public String getContent_id()    { return content_id; }
    public void   setContent_id(String v){ this.content_id = v; }
    public String getEvent_type()    { return event_type; }
    public void   setEvent_type(String v){ this.event_type = v; }
    public int    getDwell_time_ms() { return dwell_time_ms; }
    public void   setDwell_time_ms(int v){ this.dwell_time_ms = v; }
    public String getTimestamp()     { return timestamp; }
    public void   setTimestamp(String v) { this.timestamp = v; }
}
