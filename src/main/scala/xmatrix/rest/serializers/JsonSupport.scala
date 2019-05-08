package xmatrix.rest.serializers


import java.lang.reflect.InvocationTargetException

import akka.http.scaladsl.marshalling._
import akka.http.scaladsl.model.MediaTypes._
import akka.http.scaladsl.unmarshalling._
import akka.util.ByteString
import de.heikoseeberger.akkahttpjson4s.Json4sSupport
import de.heikoseeberger.akkahttpjson4s.Json4sSupport.ShouldWritePretty
import org.json4s.jackson.Serialization
import org.json4s.{DefaultFormats, Formats, MappingException, Serialization}


trait JsonSupport extends Json4sSupport {

  implicit val formats = DefaultFormats.preservingEmptyValues
  implicit val serialization = Serialization
  implicit val shouldWritePretty = ShouldWritePretty.True

  val jsonStringUnmarshaller: FromEntityUnmarshaller[String] =
    Unmarshaller.byteStringUnmarshaller
      .forContentTypes(`application/json`, `text/plain`, `application/x-www-form-urlencoded`)
      .mapWithCharset {
        case (ByteString.empty, _) => throw Unmarshaller.NoContentException
        case (data, charset) => data.decodeString(charset.nioCharset.name)
      }

  val jsonStringMarshaller: ToEntityMarshaller[String] =
    Marshaller.stringMarshaller(`application/json`)


  // HTTP entity => `A`
  override implicit def unmarshaller[A: Manifest](implicit serialization: Serialization,
                                                        formats: Formats): FromEntityUnmarshaller[A] =
    jsonStringUnmarshaller.map { data =>
      serialization.read(data)
    }.recover(
      _ =>
        _ => {
          case MappingException("unknown error",
          ite: InvocationTargetException) =>
            throw ite.getCause
        }
    )


  // `A` => HTTP entity
  override implicit def marshaller[A <: AnyRef](implicit serialization: Serialization,
                                                      formats: Formats,
                                                      shouldWritePretty: ShouldWritePretty = ShouldWritePretty.False
                                                     ): ToEntityMarshaller[A] =
    shouldWritePretty match {
      case ShouldWritePretty.False =>
        jsonStringMarshaller.compose(serialization.write[A])
      case ShouldWritePretty.True =>
        jsonStringMarshaller.compose(serialization.writePretty[A])
    }
}
