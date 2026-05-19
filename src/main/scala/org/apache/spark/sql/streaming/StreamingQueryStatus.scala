package org.apache.spark.sql.streaming

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.JsonNodeFactory

/** Reports information about the instantaneous status of a streaming query.
  *
  * @param message
  *   A human readable description of what the stream is currently doing.
  * @param isDataAvailable
  *   True when there is new data to be processed.
  * @param isTriggerActive
  *   True when the trigger is actively firing, false when waiting for the next trigger time.
  */
class StreamingQueryStatus protected[sql] (
    val message: String,
    val isDataAvailable: Boolean,
    val isTriggerActive: Boolean
) extends Serializable:

  /** The compact JSON representation of this status. */
  def json: String = StreamingQueryStatus.mapper.writeValueAsString(toJsonNode)

  /** The pretty (i.e. indented) JSON representation of this status. */
  def prettyJson: String =
    StreamingQueryStatus.mapper.writerWithDefaultPrettyPrinter().writeValueAsString(toJsonNode)

  override def toString: String = prettyJson

  private[sql] def copy(
      message: String = this.message,
      isDataAvailable: Boolean = this.isDataAvailable,
      isTriggerActive: Boolean = this.isTriggerActive
  ): StreamingQueryStatus =
    new StreamingQueryStatus(message, isDataAvailable, isTriggerActive)

  private def toJsonNode =
    val node = JsonNodeFactory.instance.objectNode()
    node.put("message", message)
    node.put("isDataAvailable", isDataAvailable)
    node.put("isTriggerActive", isTriggerActive)
    node

end StreamingQueryStatus

private[sql] object StreamingQueryStatus:
  private val mapper = new ObjectMapper()
