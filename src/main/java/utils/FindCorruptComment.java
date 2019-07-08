package utils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import utils.entity.Comment;

/**
 * this class is used to filter out corrupt tuples
 */

public class FindCorruptComment implements FilterFunction<ObjectNode> {
    @Override
    public boolean filter(ObjectNode jsonNodes) throws Exception {
        Comment comment;
        //if all field of comment is not present it means that comment is corrupt
        try {
            comment = new Comment(jsonNodes.get("value").get("approveDate").asText(), jsonNodes.get("value").get("articleId").asText(),
                    jsonNodes.get("value").get("articleWordCount").asText(), jsonNodes.get("value").get("commentID").asText(), jsonNodes.get("value").get("commentType").asText(),
                    jsonNodes.get("value").get("createDate").asText(), jsonNodes.get("value").get("depth").asText(),
                    jsonNodes.get("value").get("editorsSelection").asText(), jsonNodes.get("value").get("inReplyTo").asText(),
                    jsonNodes.get("value").get("parentUserDisplayName").asText(), jsonNodes.get("value").get("recommendations").asText(),
                    jsonNodes.get("value").get("sectionName").asText(), jsonNodes.get("value").get("userDisplayName").asText(),
                    jsonNodes.get("value").get("userID").asText(), jsonNodes.get("value").get("userLocation").asText());
        } catch (Exception e) {
            return false;
        }

        //getter of Comment class cast string value in specific type or class
        //if at least one exception is launch, it means that comment is corrupt
        try {
            comment.getApproveDate();
            comment.getArticleId();
            comment.getArticleWordCount();
            comment.getCommentID();
            comment.getCommentType();
            comment.getCreateDate();
            comment.getDepth();
            comment.getEditorsSelection();
            comment.getInReplyTo();
            comment.getParentUserDisplayName();
            comment.getRecommendations();
            comment.getSectionName();
            comment.getUserDisplayName();
            comment.getUserID();
            comment.getUserLocation();
        } catch (Exception e) {
            return false;
        }

        return true;
    }
}