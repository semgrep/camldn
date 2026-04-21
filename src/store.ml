exception Sqlite_error of { rc : string; sql : string }
exception Unsupported_scheme of string

type t = {
  uri       : Uri.t;
  in_memory : bool;
  db        : Sqlite3.db Domain.DLS.key;
  all_dbs   : Sqlite3.db list Atomic.t;
  caches    : (string, unit) Hashtbl.t;
  mutex     : Mutex.t;
}

(* Double-quote-escape a sqlite identifier (table name). *)
let escape_ident s =
  "\"" ^ String.concat "\"\"" (String.split_on_char '"' s) ^ "\""

let exec_sql db sql =
  match Sqlite3.exec db sql with
  | Sqlite3.Rc.OK -> ()
  | rc -> raise (Sqlite_error { rc = Sqlite3.Rc.to_string rc; sql })

let pragmas =
  [ (* WAL (write-ahead log) lets readers and writers run concurrently —
       critical for the per-Domain connection pool. *)
    "PRAGMA journal_mode       = WAL"
    (* NORMAL trades a tiny window of durability on OS crash for a large
       write-throughput win. Safe against app crashes; only a power loss
       can lose the last transaction. *)
  ; "PRAGMA synchronous        = NORMAL"
    (* How long a statement waits on SQLITE_BUSY before giving up (ms).
       5 s is enough to ride out routine lock contention across domains. *)
  ; "PRAGMA busy_timeout       = 5000"
    (* Enforce FOREIGN KEY REFERENCES on cache_* tables so a cache row can
       never point at a missing content sha. *)
  ; "PRAGMA foreign_keys       = ON"
    (* 256 MiB of the db mmap'd into each connection's address space —
       reads become cheap memory loads rather than syscalls. *)
  ; "PRAGMA mmap_size          = 268435456"
    (* Cap the WAL file at 64 MiB so a long-lived writer doesn't let it grow
       unbounded. WAL is truncated at the next checkpoint once below. *)
  ; "PRAGMA journal_size_limit = 67108864"
    (* Keep temp tables/indexes in RAM rather than on disk. Our workload
       only uses temps for internal sqlite things like ORDER BY, so this
       is purely a latency win. *)
  ; "PRAGMA temp_store         = MEMORY"
  ]

let bootstrap_sql =
  [ {|CREATE TABLE IF NOT EXISTS content (
       sha     BLOB PRIMARY KEY NOT NULL,
       content BLOB NOT NULL
     ) WITHOUT ROWID|}
  ]

let path_of_uri ~in_memory uri =
  if in_memory then ":memory:"
  else
    match Uri.scheme uri with
    | Some "file" | None -> Uri.path uri
    | Some s -> raise (Unsupported_scheme s)

let apply_pragmas db =
  List.iter (fun p ->
    (* PRAGMA journal_mode=WAL returns a ROW with the chosen mode; sqlite3.exec
       silently discards rows in no-callback mode, so OK is the success case. *)
    match Sqlite3.exec db p with
    | Sqlite3.Rc.OK -> ()
    | rc -> raise (Sqlite_error { rc = Sqlite3.Rc.to_string rc; sql = p }))
    pragmas

let init_conn path all_dbs =
  (* ~mutex:`NO tells sqlite3 we handle our own locking — each Domain has its
     own connection so there is no cross-thread sharing of a single handle. *)
  let db = Sqlite3.db_open ~mutex:`NO path in
  let rec add () =
    let old = Atomic.get all_dbs in
    if not (Atomic.compare_and_set all_dbs old (db :: old)) then add ()
  in
  add ();
  apply_pragmas db;
  db

let open_cdn ?(in_memory = false) uri =
  let path = path_of_uri ~in_memory uri in
  let all_dbs = Atomic.make [] in
  let db = Domain.DLS.new_key (fun () -> init_conn path all_dbs) in
  let bootstrap_db = Domain.DLS.get db in
  List.iter (exec_sql bootstrap_db) bootstrap_sql;
  { uri; in_memory; db; all_dbs;
    caches = Hashtbl.create ~random:true 16;
    mutex = Mutex.create () }

let close_cdn t =
  let rec swap () =
    let old = Atomic.get t.all_dbs in
    if Atomic.compare_and_set t.all_dbs old [] then old else swap ()
  in
  List.iter (fun db -> ignore (Sqlite3.db_close db)) (swap ())

let with_cdn ?(in_memory = false) uri f =
  let t = open_cdn ~in_memory uri in
  Fun.protect ~finally:(fun () -> close_cdn t) (fun () -> f t)

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

let content_put sha content = Effect.perform (Content_put { sha; content })
let content_get sha = Effect.perform (Content_get sha)
let ensure_cache name = Effect.perform (Ensure_cache name)
let cache_put table key_sha value_sha =
  Effect.perform (Cache_put { table; key_sha; value_sha })
let cache_get table key_sha = Effect.perform (Cache_get { table; key_sha })
let cache_delete table key_sha = Effect.perform (Cache_delete { table; key_sha })

(* --- SQL helpers for the handler --- *)

let get_conn t = Domain.DLS.get t.db

let bytes_to_blob b = Sqlite3.Data.BLOB (Bytes.to_string b)

let blob_to_bytes = function
  | Sqlite3.Data.BLOB s -> Some (Bytes.of_string s)
  (* sqlite may return TEXT for blob columns if the data was inserted as text;
     we haven't done that, but be forgiving anyway. *)
  | Sqlite3.Data.TEXT s -> Some (Bytes.of_string s)
  | _ -> None

let bind_all stmt sql bindings =
  List.iteri (fun i v ->
    match Sqlite3.bind stmt (i + 1) v with
    | Sqlite3.Rc.OK -> ()
    | rc -> raise (Sqlite_error { rc = Sqlite3.Rc.to_string rc; sql })) bindings

let step_done db stmt sql =
  match Sqlite3.step stmt with
  | Sqlite3.Rc.DONE -> Sqlite3.changes db
  | rc -> raise (Sqlite_error { rc = Sqlite3.Rc.to_string rc; sql })

let with_stmt db sql bindings f =
  let stmt = Sqlite3.prepare db sql in
  Fun.protect ~finally:(fun () -> ignore (Sqlite3.finalize stmt)) (fun () ->
    bind_all stmt sql bindings;
    f stmt)

let exec_bindings db sql bindings =
  with_stmt db sql bindings (fun stmt -> ignore (step_done db stmt sql))

let changes_bindings db sql bindings =
  with_stmt db sql bindings (fun stmt -> step_done db stmt sql)

let query_first db sql bindings =
  with_stmt db sql bindings (fun stmt ->
    match Sqlite3.step stmt with
    | Sqlite3.Rc.ROW ->
        let n = Sqlite3.column_count stmt in
        Some (List.init n (fun i -> Sqlite3.column stmt i))
    | Sqlite3.Rc.DONE -> None
    | rc -> raise (Sqlite_error { rc = Sqlite3.Rc.to_string rc; sql }))

(* --- Per-effect implementations --- *)

let do_content_put t sha content =
  let db = get_conn t in
  exec_bindings db
    "INSERT OR IGNORE INTO content(sha, content) VALUES (?, ?)"
    [ bytes_to_blob sha; bytes_to_blob content ]

let do_content_get t sha =
  let db = get_conn t in
  match
    query_first db
      "SELECT content FROM content WHERE sha = ?"
      [ bytes_to_blob sha ]
  with
  | Some [c] -> blob_to_bytes c
  | _ -> None

let do_ensure_cache t name =
  Mutex.lock t.mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock t.mutex) (fun () ->
    if not (Hashtbl.mem t.caches name) then begin
      let db = get_conn t in
      let sql =
        Printf.sprintf {|CREATE TABLE IF NOT EXISTS %s (
            key_sha   BLOB PRIMARY KEY NOT NULL,
            value_sha BLOB NOT NULL,
            FOREIGN KEY (key_sha)   REFERENCES content(sha),
            FOREIGN KEY (value_sha) REFERENCES content(sha)
          ) WITHOUT ROWID|}
          (escape_ident name)
      in
      exec_sql db sql;
      Hashtbl.replace t.caches name ()
    end)

let do_cache_put t table key_sha value_sha =
  let db = get_conn t in
  let sql =
    Printf.sprintf
      "INSERT OR REPLACE INTO %s(key_sha, value_sha) VALUES (?, ?)"
      (escape_ident table)
  in
  exec_bindings db sql [ bytes_to_blob key_sha; bytes_to_blob value_sha ]

let do_cache_get t table key_sha =
  let db = get_conn t in
  let sql =
    Printf.sprintf
      "SELECT value_sha FROM %s WHERE key_sha = ?"
      (escape_ident table)
  in
  match query_first db sql [ bytes_to_blob key_sha ] with
  | Some [c] -> blob_to_bytes c
  | _ -> None

let do_cache_delete t table key_sha =
  let db = get_conn t in
  let sql =
    Printf.sprintf "DELETE FROM %s WHERE key_sha = ?" (escape_ident table)
  in
  let n = changes_bindings db sql [ bytes_to_blob key_sha ] in
  n > 0

let with_handlers (type a) (t : t) (f : t -> a) : a =
  let open Effect.Deep in
  try_with f t
    { effc = (fun (type c) (eff : c Effect.t) ->
        match eff with
        | Content_put { sha; content } ->
            Some (fun (k : (c, _) continuation) ->
              do_content_put t sha content;
              continue k ())
        | Content_get sha ->
            Some (fun (k : (c, _) continuation) ->
              continue k (do_content_get t sha))
        | Ensure_cache name ->
            Some (fun (k : (c, _) continuation) ->
              do_ensure_cache t name;
              continue k ())
        | Cache_put { table; key_sha; value_sha } ->
            Some (fun (k : (c, _) continuation) ->
              do_cache_put t table key_sha value_sha;
              continue k ())
        | Cache_get { table; key_sha } ->
            Some (fun (k : (c, _) continuation) ->
              continue k (do_cache_get t table key_sha))
        | Cache_delete { table; key_sha } ->
            Some (fun (k : (c, _) continuation) ->
              continue k (do_cache_delete t table key_sha))
        | _ -> None) }
