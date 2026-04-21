(** Low-level connection + effect layer.

    No typed schema, no column ADT. Just raw bytes in/out, keyed by SHA. *)

exception Sqlite_error of { rc : string; sql : string }
exception Unsupported_scheme of string

type t
(** A handle to an open sqlite3-backed store. Internally:
    - one [Sqlite3.db] per Domain (via [Domain.DLS])
    - an [Atomic.t] of every opened handle, for [close_cdn]
    - a mutex-guarded hashtable tracking which cache tables have been ensured
*)

val open_cdn : ?in_memory:bool -> Uri.t -> t
(** Open the sqlite file at [uri]. If [in_memory=true], ignores the uri and
    opens [":memory:"] — single-Domain only. Applies performance pragmas and
    creates the [content] table if absent. *)

val close_cdn : t -> unit
(** Close every sqlite handle opened under [t]. Safe to call once per [t]. *)

val with_cdn : ?in_memory:bool -> Uri.t -> (t -> 'a) -> 'a

(** {1 Effects}

    Every CRUD is an algebraic effect. Perform them after installing
    [with_handlers]. *)

type _ Effect.t +=
  | Content_put :
      { sha : bytes; content : bytes } -> unit Effect.t
  | Content_get :
      bytes -> bytes option Effect.t
  | Ensure_cache :
      string -> unit Effect.t
  | Cache_put :
      { table : string; key_sha : bytes; value_sha : bytes } -> unit Effect.t
  | Cache_get :
      { table : string; key_sha : bytes } -> bytes option Effect.t
  | Cache_delete :
      { table : string; key_sha : bytes } -> bool Effect.t

(** {1 Direct API}

    One-line wrappers around [Effect.perform] for callers who'd rather not
    spell out the effect constructor each time. *)

val content_put  : bytes -> bytes -> unit
val content_get  : bytes -> bytes option
val ensure_cache : string -> unit
val cache_put    : string -> bytes -> bytes -> unit
val cache_get    : string -> bytes -> bytes option
val cache_delete : string -> bytes -> bool

val with_handlers : t -> (t -> 'a) -> 'a
(** Install the default sqlite-backed handler for every effect above, for the
    scope of [f]. Exceptions raised inside the handler bodies propagate out of
    [with_handlers]. *)
