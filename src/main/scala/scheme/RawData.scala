package scheme

import java.time.Instant

trait RawData{
  val user_id: Int
  val item_id: Int
  val category_id: Int
  val behavior: String
  val ts: Instant
}