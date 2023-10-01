package scheme

import io.circe.{Decoder, DecodingFailure, HCursor}
import io.circe.generic.extras.Configuration

import java.time.{Instant, LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter
import scala.util.Try




case class Event(
                  user_id: Int,
                  item_id: Int,
                  category_id: Int,
                  behavior: String,
                  ts: Instant) extends RawData


object Event {
  implicit val customConfig: Configuration = Configuration.default.withDefaults

  implicit val decoder: Decoder[Event] = new Decoder[Event] {
    final def apply(c: HCursor): Decoder.Result[Event] =

      for {
        user_id <- c.downField("user_id").as[Int]
        item_id <- c.downField("item_id").as[Int]
        category_id <- c.downField("category_id").as[Int]
        behavior <- c.downField("behavior").as[String]
        tsStr <- c.downField("ts").as[String]
        ts <-
          Try(
            LocalDateTime.parse(
              tsStr,
              DateTimeFormatter.ofPattern(Constants.defaultFormat
              )
            ).toInstant(ZoneOffset.UTC))
          .toEither
          .left.map(e => DecodingFailure(e.getMessage, c.history))

      } yield Event(user_id, item_id, category_id, behavior, ts)
  }
}