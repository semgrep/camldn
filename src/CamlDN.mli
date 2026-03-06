(** Stores the DB connection info. Has a pool of connections, specifically one for each domain *)
type t

val open_cdn : ?in_memory:bool ->  Uri.t -> t
(** [open_cdn uri] will check if the backing sqlite3 table exists at the uri *)

val close_cdn : t -> unit
(** [close_cdn camldn] will close the connection to the sqlite3 table, and free up any resources. *)

val with_cdn : ?in_memory:bool -> Uri.t -> (t -> 'a) -> 'a
(** [with_cdn uri f ] is like [open_cdn uri |> f |> close_cdn], but ensures that the connection is properly closed even if [f] raises an exception. *)

val with_handlers : t -> (t -> 'a) -> 'a
(** [with_handlers camldn f] is a helper function that ensures that effect handlers are setup *)






(** Define the name of the cache and how to calculate the cache key, and serialize/deserialize the underlying type *)
module type CacheType = sig
  (** the parameter to cache by *)
  type param

  (** the resulting value  *)
  type value

  (** name of the cache kind, to be used as the sqlite table name*)
  val name : string

  (** convert the param into the string to check for it in the db *)
  val key_of_param : param -> string

  (** Convert the value into the bytes if we need to store the result *)
  val bytes_of_value : value -> bytes

  (** Convert the value back from bytes if we find it in the cache *)
  val value_of_bytes : bytes -> value
end

module type Cache = sig
  (** Same as [CacheType.param]*)
  type param

    (** Same as [CacheType.value]*)
  type value

  val maybe_do : f_name:string -> f:(param -> value) -> param -> value
  (** [maybe_do ~f_name ~f x] checks if the result of [f x], indicated by
      [f_name, CacheType.key_of_param x], is in the cache. It does this by
      performing an effect. If so, it returns the cached value. Otherwise, it
      computes [f x], stores it in the cache through an effect, and returns the
      result. It will also create the underlying sqlite3 table if necessary. *)
end

module Make (C : CacheType) : Cache with type param = C.param and type value = C.value

module MakeMarshal (C : sig
  type param
  type value
  val name : string
  val key_of_param : param -> string
  val closures : bool
end) : Cache with type param = C.param and type value = C.value
(** like [Make], but uses [Marshal] for converting the value back and forth *)
