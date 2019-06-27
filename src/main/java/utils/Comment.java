package utils;

import java.io.Serializable;
import java.util.Date;
import java.util.TimeZone;

public class Comment implements Serializable {
    private static final long serialVersionUID = 1L;

    private String approveDate;
    private String articleId;
    private String articleWordCount;
    private String commentID;
    private String commentType;
    private String createDate;
    private String depth;
    private String editorsSelection;
    private String inReplyTo;
    private String parentUserDisplayName;
    private String recommendations;
    private String sectionName;
    private String userDisplayName;
    private String userID;
    private String userLocation;


    public Comment() {
    }

    public Comment(String articleId, String createDate) {
        this.articleId = articleId;
        this.createDate = createDate;
    }

    public Comment(String approveDate, String articleId, String articleWordCount, String commentID,
                   String commentType, String createDate, String depth, String editorsSelection,
                   String inReplyTo, String parentUserDisplayName, String recommendations,
                   String sectionName, String userDisplayName, String userID, String userLocation) {
        this.approveDate = approveDate;
        this.articleId = articleId;
        this.articleWordCount = articleWordCount;
        this.commentID = commentID;
        this.commentType = commentType;
        this.createDate = createDate;
        this.depth = depth;
        this.editorsSelection = editorsSelection;
        this.inReplyTo = inReplyTo;
        this.parentUserDisplayName = parentUserDisplayName;
        this.recommendations = recommendations;
        this.sectionName = sectionName;
        this.userDisplayName = userDisplayName;
        this.userID = userID;
        this.userLocation = userLocation;
    }

    public static Date fromUnixTimeToUTC(String timestamp){
        long ts = Long.valueOf(timestamp)*1000;
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        return new Date(ts);
    }
    public static Date fromUnixTimeToUTC2(long timestamp){
        long ts = timestamp*1000;
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        return new Date(ts);
    }

    public long getApproveDate() {
        return fromUnixTimeToUTC(approveDate).getTime();
    }

    public void setApproveDate(String approveDate) {
        this.approveDate = approveDate;
    }

    public String getArticleId() {
        return articleId;
    }

    public void setArticleId(String articleId) {
        this.articleId = articleId;
    }

    public long getCreateDate() {
        return fromUnixTimeToUTC(createDate).getTime();
    }

    public void setCreateDate(String createDate) {
        this.createDate = createDate;
    }

    public int getCommentID() {
        return Integer.parseUnsignedInt(commentID);
    }

    public void setCommentID(String commentID) {
        this.commentID = commentID;
    }


    public int getArticleWordCount() {
        return Integer.parseUnsignedInt(articleWordCount);
    }

    public void setArticleWordCount(String articleWordCount) {
        this.articleWordCount =articleWordCount;
    }

    public String getCommentType() {
        return commentType;
    }

    public void setCommentType(String commentType) {
        this.commentType = commentType;
    }

    public int getDepth() {
        return Integer.parseInt(depth);
    }

    public void setDepth(String depth) {
        this.depth = depth;
    }

    public boolean getEditorsSelection() {
        return Boolean.parseBoolean(editorsSelection);
    }

    public void setEditorsSelection(String editorsSelection) {
        this.editorsSelection = editorsSelection;
    }

    public int getInReplyTo() {
        return Integer.parseUnsignedInt(inReplyTo);
    }

    public void setInReplyTo(String inReplyTo) {
        this.inReplyTo=inReplyTo;
    }

    public String getParentUserDisplayName() {
        return parentUserDisplayName;
    }

    public void setParentUserDisplayName(String parentUserDisplayName) {
        this.parentUserDisplayName = parentUserDisplayName;
    }

    public int getRecommendations() {
        return Integer.parseInt(recommendations);
    }

    public void setRecommendations(String recommendations) {
        this.recommendations = recommendations;
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
        return Integer.parseUnsignedInt(userID);
    }

    public void setUserID(String userID) {
        this.userID =  userID;
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
                ", editorsSelection=" + editorsSelection +
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
