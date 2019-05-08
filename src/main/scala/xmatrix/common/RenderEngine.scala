package xmatrix.common

import java.io.{StringReader, StringWriter}
import org.apache.velocity.VelocityContext
import org.apache.velocity.app.Velocity


object RenderEngine {
  def render(templateStr: String, root: Map[String, AnyRef]) = {
    val context: VelocityContext = new VelocityContext
    root.map { f =>
      context.put(f._1, f._2)
    }
    val w: StringWriter = new StringWriter
    Velocity.evaluate(context, w, "", new StringReader(templateStr))
    w.toString
  }
}
