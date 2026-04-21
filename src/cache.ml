module type S = sig
  val table_name  : string
  type key
  type value
  val key_serde   : key Serde.t
  val value_serde : value Serde.t
end

module Make (C : S) = struct
  let ensure_table () = Store.ensure_cache C.table_name

  let put k v =
    ensure_table ();
    let ks = C.key_serde.store k in
    let vs = C.value_serde.store v in
    Store.cache_put C.table_name ks vs

  let get_opt k =
    ensure_table ();
    let ks = C.key_serde.store k in
    match Store.cache_get C.table_name ks with
    | None    -> None
    | Some vs -> Some (C.value_serde.load vs)

  let delete k =
    ensure_table ();
    let ks = C.key_serde.store k in
    Store.cache_delete C.table_name ks
end
