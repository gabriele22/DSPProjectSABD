package utils;

import java.io.Serializable;
import java.util.Date;
import java.util.TimeZone;

public class Comment implements Serializable {
    private static final long serialVersionUID = 1L;

    private Date approveDate;
    private String articleId;
    private int articleWordCount;
    private int commentID;
    private String commentType;
    private Date createDate;
    private int depth;
    private boolean editorSelection;
    private int inReplyTo;
    private String parentUserDisplayName;
    private int recommendations;
    private String sectionName;
    private String userDisplayName;
    private int userID;
    private String userLocation;


    public Comment() {
    }

    public Comment(String approveDate, String articleId, String articleWordCount, String commentID, String commentType,
                   String createDate, String depth, String editorSelection, String inReplyTo, String parentUserDisplayName,
                   String recommendations, String sectionName, String userDisplayName, String userID, String userLocation) {
        this.approveDate = fromUnixTimeToUTC(approveDate);
        this.articleId = articleId;
        this.articleWordCount = Integer.parseUnsignedInt(articleWordCount);
        this.commentID = Integer.parseUnsignedInt(commentID);
        this.commentType = commentType;
        this.createDate = fromUnixTimeToUTC(createDate);
        this.depth = Integer.parseInt(depth);
        this.editorSelection = Boolean.parseBoolean(editorSelection);
        this.inReplyTo = Integer.parseUnsignedInt(inReplyTo);
        this.parentUserDisplayName = parentUserDisplayName;
        this.recommendations = Integer.parseInt(recommendations);
        this.sectionName = sectionName;
        this.userDisplayName = userDisplayName;
        this.userID = Integer.parseUnsignedInt(userID);
        this.userLocation = userLocation;
    }

    private static Date fromUnixTimeToUTC(String timestamp){
        long ts = Long.valueOf(timestamp)*1000;
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        return new Date(ts);
    }

    public Date getApproveDate() {
        return approveDate;
    }

    public void setApproveDate(String approveDate) {
        this.approveDate = fromUnixTimeToUTC(approveDate);
    }

    public String getArticleId() {
        return articleId;
    }

    public void setArticleId(String articleId) {
        this.articleId = articleId;
    }

    public Date getCreateDate() {
        return createDate;
    }

    public void setCreateDate(String createDate) {
        this.createDate = fromUnixTimeToUTC(createDate);
    }

    public int getCommentID() {
        return commentID;
    }

    public void setCommentID(String commentID) {
        this.commentID = Integer.parseUnsignedInt(commentID);
    }


    public int getArticleWordCount() {
        return articleWordCount;
    }

    public void setArticleWordCount(String articleWordCount) {
        this.articleWordCount = Integer.parseUnsignedInt(articleWordCount);
    }

    public String getCommentType() {
        return commentType;
    }

    public void setCommentType(String commentType) {
        this.commentType = commentType;
    }

    public int getDepth() {
        return depth;
    }

    public void setDepth(String depth) {
        this.depth = Integer.parseInt(depth);
    }

    public boolean isEditorSelection() {
        return editorSelection;
    }

    public void setEditorSelection(String  editorSelection) {
        this.editorSelection = Boolean.parseBoolean(editorSelection);
    }

    public int getInReplyTo() {
        return inReplyTo;
    }

    public void setInReplyTo(String inReplyTo) {
        if (!inReplyTo.equals(""))
            this.inReplyTo =  Integer.parseUnsignedInt(inReplyTo);
    }

    public String getParentUserDisplayName() {
        return parentUserDisplayName;
    }

    public void setParentUserDisplayName(String parentUserDisplayName) {
        this.parentUserDisplayName = parentUserDisplayName;
    }

    public int getRecommendations() {
        return recommendations;
    }

    public void setRecommendations(String recommendations) {
        this.recommendations = Integer.parseInt(recommendations);
    }

    public String getSectionName() {
        return sectionName;
    }

    public void setSectionName(String sectionName) {
        this.sectionName = sectionName;
    }

    public String getUserDisplayName() {
        return userDisplayName;
    }

    public void setUserDisplayName(String userDisplayName) {
        this.userDisplayName = userDisplayName;
    }

    public int getUserID() {
        return userID;
    }

    public void setUserID(String userID) {
        this.userID =  Integer.parseUnsignedInt(userID);
    }

    public String getUserLocation() {
        return userLocation;
    }

    public void setUserLocation(String userLocation) {
        this.userLocation = userLocation;
    }

    @Override
    public String toString() {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        return "Comment{" +
                "approveDate=" + approveDate.toString() +
                ", articleId='" + articleId + '\'' +
                ", articleWordCount=" + articleWordCount +
                ", commentID=" + commentID +
                ", commentType='" + commentType + '\'' +
                ", createDate=" + createDate.toString() +
                ", depth=" + depth +
                ", editorSelection=" + editorSelection +
                ", inReplyTo=" + inReplyTo +
                ", parentUserDisplayName='" + parentUserDisplayName + '\'' +
                ", recommendations=" + recommendations +
                ", sectionName='" + sectionName + '\'' +
                ", userDisplayName='" + userDisplayName + '\'' +
                ", userID=" + userID +
                ", userLocation='" + userLocation + '\'' +
                '}';
    }
}
