exception Sqlite_error of { rc : string; sql : string }
exception Unsupported_scheme of string

(* Internal effects — not exposed in the .mli *)
type _ Effect.t +=
  | CacheEnsureTable : string -> unit Effect.t
  | CacheGet : (string * string) -> bytes option Effect.t
  | CacheSet : (string * string * bytes) -> unit Effect.t

type t = {
  path : string;
  conn_key : Sqlite3.db Domain.DLS.key;
  all_conns : Sqlite3.db list Atomic.t;
  tables : (string, unit) Hashtbl.t;
  table_mutex : Mutex.t;
}

(* --- Internal helpers --- *)

let get_conn t = Domain.DLS.get t.conn_key

(* sqlite3 identifiers (table names etc.) can't be parameterized with ?,
   so we quote them manually. Double-quoting is the SQL standard way to
   escape identifiers: any embedded double-quote is doubled. *)
let escape_ident s =
  "\"" ^ String.concat "\"\"" (String.split_on_char '"' s) ^ "\""

(* sqlite3-ocaml's [exec] returns an Rc.t rather than raising, so we have
   to check the return code ourselves. *)
let exec_sql db sql =
  match Sqlite3.exec db sql with
  | Sqlite3.Rc.OK -> ()
  | rc -> raise (Sqlite_error { rc = Sqlite3.Rc.to_string rc; sql })

let ensure_table t name =
  Mutex.lock t.table_mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock t.table_mutex) (fun () ->
    if not (Hashtbl.mem t.tables name) then begin
      let db = get_conn t in
      let sql =
        "CREATE TABLE IF NOT EXISTS " ^ escape_ident name
        ^ " (key TEXT PRIMARY KEY NOT NULL, value BLOB NOT NULL)"
      in
      exec_sql db sql;
      Hashtbl.replace t.tables name ()
    end)

let cache_get t table key =
  let db = get_conn t in
  let sql =
    "SELECT value FROM " ^ escape_ident table ^ " WHERE key = ?"
  in
  (* sqlite3-ocaml prepared statements are not reusable across bindings
     without a reset, so we create+finalize each time. The sqlite3 engine
     caches the compiled bytecode internally anyway. *)
  let stmt = Sqlite3.prepare db sql in
  Fun.protect ~finally:(fun () -> ignore (Sqlite3.finalize stmt)) (fun () ->
    (* bind_text/bind_blob return Rc.t — OK on success. We ignore because
       a bind failure will just surface as a step failure below. *)
    ignore (Sqlite3.bind_text stmt 1 key);
    match Sqlite3.step stmt with
    | Sqlite3.Rc.ROW ->
      (* sqlite3-ocaml returns BLOB columns as Data.BLOB, but if the value
         was inserted via bind_text it may come back as Data.TEXT. We accept
         both so round-tripping works regardless of affinity. *)
      (match Sqlite3.column stmt 0 with
       | Sqlite3.Data.BLOB b -> Some (Bytes.of_string b)
       | Sqlite3.Data.TEXT s -> Some (Bytes.of_string s)
       | _ -> None)
    | _ -> None)

let cache_set t table key value =
  let db = get_conn t in
  (* INSERT OR REPLACE is sqlite-specific syntax — it does an upsert keyed
     on the PRIMARY KEY, so repeat sets for the same key just overwrite. *)
  let sql =
    "INSERT OR REPLACE INTO " ^ escape_ident table
    ^ " (key, value) VALUES (?, ?)"
  in
  let stmt = Sqlite3.prepare db sql in
  Fun.protect ~finally:(fun () -> ignore (Sqlite3.finalize stmt)) (fun () ->
    ignore (Sqlite3.bind_text stmt 1 key);
    (* bind_blob takes a string (not bytes) because sqlite3-ocaml predates
       the bytes/string split — so we must Bytes.to_string here. *)
    ignore (Sqlite3.bind_blob stmt 2 (Bytes.to_string value));
    match Sqlite3.step stmt with
    | Sqlite3.Rc.DONE -> ()
    | rc ->
      raise (Sqlite_error { rc = Sqlite3.Rc.to_string rc; sql }))

(* --- Connection lifecycle --- *)

let open_cdn ?(in_memory = false) uri =
  let path =
    if in_memory then ":memory:"
    else
      match Uri.scheme uri with
      | Some "file" -> Uri.path uri
      | None -> Uri.path uri
      | Some s -> raise (Unsupported_scheme s)
  in
  let all_conns = Atomic.make [] in
  let conn_key =
    Domain.DLS.new_key (fun () ->
      (* ~mutex:`NO tells sqlite3 we handle our own locking.
         Each Domain gets its own connection via DLS so there is no
         cross-thread sharing of a single db handle. *)
      let db = Sqlite3.db_open ~mutex:`NO path in
      (* CAS loop to atomically prepend to the connection list so
         close_cdn can find and close every connection later. *)
      let rec add_conn () =
        let old = Atomic.get all_conns in
        if not (Atomic.compare_and_set all_conns old (db :: old))
        then add_conn ()
      in
      add_conn ();
      (* WAL (write-ahead logging) lets readers and writers proceed
         concurrently — important since each Domain has its own conn. *)
      exec_sql db "PRAGMA journal_mode=WAL";
      db)
  in
  {
    path;
    conn_key;
    all_conns;
    tables = Hashtbl.create 16;
    table_mutex = Mutex.create ();
  }

let close_cdn t =
  (* CAS-swap the conn list to [] and close everything we took.
     db_close returns bool (false = busy) — we ignore it because
     sqlite3-ocaml will finalize remaining stmts on GC anyway. *)
  let rec swap () =
    let old = Atomic.get t.all_conns in
    if not (Atomic.compare_and_set t.all_conns old [])
    then swap ()
    else old
  in
  let conns = swap () in
  List.iter (fun db -> ignore (Sqlite3.db_close db)) conns

let with_cdn ?(in_memory = false) uri f =
  let t = open_cdn ~in_memory uri in
  Fun.protect ~finally:(fun () -> close_cdn t) (fun () -> f t)

(* --- Effect handler --- *)

let with_handlers t f =
  Effect.Deep.try_with f t
    { effc = fun (type a) (eff : a Effect.t) ->
        match eff with
        | CacheEnsureTable name ->
          Some (fun (k : (a, _) Effect.Deep.continuation) ->
            ensure_table t name;
            Effect.Deep.continue k ())
        | CacheGet (table, key) ->
          Some (fun (k : (a, _) Effect.Deep.continuation) ->
            let result = cache_get t table key in
            Effect.Deep.continue k result)
        | CacheSet (table, key, value) ->
          Some (fun (k : (a, _) Effect.Deep.continuation) ->
            cache_set t table key value;
            Effect.Deep.continue k ())
        | _ -> None }

(* --- Module types and functors --- *)

module type CacheType = sig
  type param
  type value
  val name : string
  val key_of_param : param -> string
  val bytes_of_value : value -> bytes
  val value_of_bytes : bytes -> value
end

module type Cache = sig
  type param
  type value
  val maybe_do : f_name:string -> f:(param -> value) -> param -> value
end

(* Shared memoization core — both Make and MakeMarshal delegate here.
   [to_bytes] and [of_bytes] are the only things that differ between the
   two functors, so we parameterize over them. *)
let maybe_do_generic ~name ~key_of_param ~to_bytes ~of_bytes ~f_name ~f param =
  (* The null byte separator ensures "foo" + "bar" and "fo" + "obar"
     produce distinct composite keys. *)
  let key = f_name ^ "\x00" ^ key_of_param param in
  Effect.perform (CacheEnsureTable name);
  match Effect.perform (CacheGet (name, key)) with
  | Some bytes -> of_bytes bytes
  | None ->
    let result = f param in
    let bytes = to_bytes result in
    Effect.perform (CacheSet (name, key, bytes));
    result

module Make (C : CacheType) : Cache with type param = C.param and type value = C.value = struct
  type param = C.param
  type value = C.value

  let maybe_do ~f_name ~f param =
    maybe_do_generic
      ~name:C.name ~key_of_param:C.key_of_param
      ~to_bytes:C.bytes_of_value ~of_bytes:C.value_of_bytes
      ~f_name ~f param
end

module MakeMarshal (C : sig
  type param
  type value
  val name : string
  val key_of_param : param -> string
  val closures : bool
end) : Cache with type param = C.param and type value = C.value = struct
  type param = C.param
  type value = C.value

  let marshal_flags = if C.closures then [Marshal.Closures] else []

  let maybe_do ~f_name ~f param =
    maybe_do_generic
      ~name:C.name ~key_of_param:C.key_of_param
      ~to_bytes:(fun v -> Marshal.to_bytes (v : C.value) marshal_flags)
      ~of_bytes:(fun b -> (Marshal.from_bytes b 0 : C.value))
      ~f_name ~f param
end
