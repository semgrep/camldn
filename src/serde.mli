(** Content-addressed serialization: bin_prot for primitive leaves, merkle
    encoding for composites (polymorphic ADTs, variants, records). *)

exception Missing_content of bytes
(** Raised by [load] when the backing content row is absent. *)

type 'a t = {
  store : 'a -> bytes;
  (** Canonically encode [x], recursively store every child via
      [Store.Content_put], return the root SHA (32 bytes). *)
  load : bytes -> 'a;
  (** Fetch the blob for the root SHA via [Store.Content_get], parse,
      recursively fetch children. Raises [Missing_content] on miss. *)
}

(** {1 Primitives (bin_prot)} *)

val int    : int t
val int64  : int64 t
val string : string t
val float  : float t
val bool   : bool t
val bytes  : bytes t
val unit   : unit t

(** {1 Polymorphic combinators (merkle-encoded)} *)

val option : 'a t -> 'a option t
val list   : 'a t -> 'a list t
val result : 'a t -> 'b t -> ('a, 'b) result t
val pair   : 'a t -> 'b t -> ('a * 'b) t
val triple : 'a t -> 'b t -> 'c t -> ('a * 'b * 'c) t
val quad   : 'a t -> 'b t -> 'c t -> 'd t -> ('a * 'b * 'c * 'd) t

val ref : 'a t -> 'a ref t
(** Serialize an ['a ref] as its contents wrapped in a merkle blob. {b Aliasing
    is not preserved}: if two refs in the source data point at the same cell,
    [load] returns two independent fresh refs. Later mutation through one will
    not be visible through the other. *)

val lazy_ : 'a t -> 'a Lazy.t t
(** Serialize a lazy value by {b forcing it at store time} and storing the
    result. [load] returns a [Lazy.from_val]-style already-forced lazy.

    Caveats:
    - Any side effects in the thunk run during [store], not during [load].
    - If the thunk raises, the exception escapes [store].
    - Laziness is not preserved across the round-trip; the returned lazy is
      always already forced.
    - Sharing of the underlying thunk by physical identity is not preserved
      (value-level sharing via content-addressing still collapses equal
      forced results). *)

(** {1 Escape hatch and merkle primitives} *)

val of_bin_prot : 'a Bin_prot.Type_class.t -> 'a t
(** Build a primitive serde from a bin_prot type-class. The payload is the raw
    bin_prot bytes of [x]. *)

val sha256 : bytes -> bytes
(** SHA-256 of the argument, returned as a 32-byte [bytes]. *)

module Merkle : sig
  (** Canonical blob layout:
      {v
      [ 0x01 (1B version) | tag (uint16 LE) | n (uint32 LE)
        | sha_1 (32B) | … | sha_n (32B) ]
      v} *)

  val store : tag:int -> bytes list -> bytes
  (** Build a merkle blob with the given [tag] and [children] SHAs, store it
      via [Content_put], and return the root SHA. Each child SHA must be
      exactly 32 bytes. *)

  val load : bytes -> (int * bytes list) option
  (** Fetch + parse. [None] if the SHA is missing or the blob doesn't look
      like a merkle payload. *)
end
