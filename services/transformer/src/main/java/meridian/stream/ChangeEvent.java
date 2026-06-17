package meridian.stream;

public class ChangeEvent {
    private long id; private String type; private int namespace;
    private String title; private String titleUrl; private String comment;
    private long timestamp; private String user; private boolean bot;
    private String serverUrl; private String serverName; private String serverScriptUrl;
    private String wiki; private long parsedTimestamp;

    public ChangeEvent() {}
    public ChangeEvent(long id, String type, int ns, String title, String titleUrl,
                       String comment, long ts, String user, boolean bot,
                       String serverUrl, String serverName, String serverScriptUrl,
                       String wiki, long parsedTs) {
        this.id=id; this.type=type; this.namespace=ns; this.title=title; this.titleUrl=titleUrl;
        this.comment=comment; this.timestamp=ts; this.user=user; this.bot=bot;
        this.serverUrl=serverUrl; this.serverName=serverName; this.serverScriptUrl=serverScriptUrl;
        this.wiki=wiki; this.parsedTimestamp=parsedTs;
    }
    public long getId() { return id; } public void setId(long id) { this.id = id; }
    public String getType() { return type; } public void setType(String type) { this.type = type; }
    public int getNamespace() { return namespace; } public void setNamespace(int namespace) { this.namespace = namespace; }
    public String getTitle() { return title; } public void setTitle(String title) { this.title = title; }
    public String getTitleUrl() { return titleUrl; } public void setTitleUrl(String titleUrl) { this.titleUrl = titleUrl; }
    public String getComment() { return comment; } public void setComment(String comment) { this.comment = comment; }
    public long getTimestamp() { return timestamp; } public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
    public String getUser() { return user; } public void setUser(String user) { this.user = user; }
    public boolean isBot() { return bot; } public void setBot(boolean bot) { this.bot = bot; }
    public String getServerUrl() { return serverUrl; } public void setServerUrl(String serverUrl) { this.serverUrl = serverUrl; }
    public String getServerName() { return serverName; } public void setServerName(String serverName) { this.serverName = serverName; }
    public String getServerScriptUrl() { return serverScriptUrl; } public void setServerScriptUrl(String serverUrl) { this.serverScriptUrl = serverUrl; }
    public String getWiki() { return wiki; } public void setWiki(String wiki) { this.wiki = wiki; }
    public long getParsedTimestamp() { return parsedTimestamp; } public void setParsedTimestamp(long ts) { this.parsedTimestamp = ts; }
    @Override public String toString() { return "ChangeEvent{id="+id+", title='"+title+"', user='"+user+"'}"; }
}
