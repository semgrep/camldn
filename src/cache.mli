(** A typed cache over the content-addressed store.

    Define a module conforming to [S] for each (key, value) pair you want to
    cache, then [Make] it to get [put] / [get_opt] / [delete] / [ensure_table].
*)

module type S = sig
  val table_name  : string
  type key
  type value
  val key_serde   : key Serde.t
  val value_serde : value Serde.t
end

module Make (C : S) : sig
  val ensure_table : unit -> unit
  val put          : C.key -> C.value -> unit
  val get_opt      : C.key -> C.value option
  val delete       : C.key -> bool
end
