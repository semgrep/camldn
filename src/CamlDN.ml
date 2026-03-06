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

let escape_ident s =
  "\"" ^ String.concat "\"\"" (String.split_on_char '"' s) ^ "\""

let exec_sql db sql =
  match Sqlite3.exec db sql with
  | Sqlite3.Rc.OK -> ()
  | rc -> failwith ("SQLite error: " ^ Sqlite3.Rc.to_string rc ^ " on: " ^ sql)

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
  let stmt = Sqlite3.prepare db sql in
  Fun.protect ~finally:(fun () -> ignore (Sqlite3.finalize stmt)) (fun () ->
    ignore (Sqlite3.bind_text stmt 1 key);
    match Sqlite3.step stmt with
    | Sqlite3.Rc.ROW ->
      (match Sqlite3.column stmt 0 with
       | Sqlite3.Data.BLOB b -> Some (Bytes.of_string b)
       | Sqlite3.Data.TEXT s -> Some (Bytes.of_string s)
       | _ -> None)
    | _ -> None)

let cache_set t table key value =
  let db = get_conn t in
  let sql =
    "INSERT OR REPLACE INTO " ^ escape_ident table
    ^ " (key, value) VALUES (?, ?)"
  in
  let stmt = Sqlite3.prepare db sql in
  Fun.protect ~finally:(fun () -> ignore (Sqlite3.finalize stmt)) (fun () ->
    ignore (Sqlite3.bind_text stmt 1 key);
    ignore (Sqlite3.bind_blob stmt 2 (Bytes.to_string value));
    match Sqlite3.step stmt with
    | Sqlite3.Rc.DONE -> ()
    | rc -> failwith ("SQLite insert error: " ^ Sqlite3.Rc.to_string rc))

(* --- Connection lifecycle --- *)

let open_cdn ?(in_memory = false) uri =
  let path =
    if in_memory then ":memory:"
    else
      match Uri.scheme uri with
      | Some "file" -> Uri.path uri
      | None -> Uri.path uri
      | Some s -> failwith ("Unsupported URI scheme: " ^ s)
  in
  let all_conns = Atomic.make [] in
  let conn_key =
    Domain.DLS.new_key (fun () ->
      let db = Sqlite3.db_open ~mutex:`NO path in
      (* Atomically prepend to connection list *)
      let rec add_conn () =
        let old = Atomic.get all_conns in
        if not (Atomic.compare_and_set all_conns old (db :: old))
        then add_conn ()
      in
      add_conn ();
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

module Make (C : CacheType) : Cache with type param = C.param and type value = C.value = struct
  type param = C.param
  type value = C.value

  let maybe_do ~f_name ~f param =
    let key = f_name ^ "\x00" ^ C.key_of_param param in
    Effect.perform (CacheEnsureTable C.name);
    match Effect.perform (CacheGet (C.name, key)) with
    | Some bytes -> C.value_of_bytes bytes
    | None ->
      let result = f param in
      let bytes = C.bytes_of_value result in
      Effect.perform (CacheSet (C.name, key, bytes));
      result
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

  let marshal_flags c = if c then [Marshal.Closures] else []

  let maybe_do ~f_name ~f param =
    let key = f_name ^ "\x00" ^ C.key_of_param param in
    Effect.perform (CacheEnsureTable C.name);
    match Effect.perform (CacheGet (C.name, key)) with
    | Some bytes -> (Marshal.from_bytes bytes 0 : C.value)
    | None ->
      let result = f param in
      let bytes = Marshal.to_bytes (result : C.value) (marshal_flags C.closures) in
      Effect.perform (CacheSet (C.name, key, bytes));
      result
end
