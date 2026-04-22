module Store = Camldn.Store
module SerDe = Camldn.SerDe
module Cache = Camldn.Cache
module Memo  = Camldn.Memo

let with_in_memory f =
  Store.with_cdn ~in_memory:true (Uri.of_string "") (fun t ->
    Store.with_handlers t (fun _ -> f t))

let with_temp_file_db f =
  let path = Filename.temp_file "camldn_test_" ".db" in
  Sys.remove path;
  let uri = Uri.of_string ("file://" ^ path) in
  Fun.protect
    ~finally:(fun () ->
      if Sys.file_exists path then Sys.remove path;
      let wal = path ^ "-wal" in
      let shm = path ^ "-shm" in
      if Sys.file_exists wal then Sys.remove wal;
      if Sys.file_exists shm then Sys.remove shm)
    (fun () -> f path uri)

let count_rows db sql =
  let stmt = Sqlite3.prepare db sql in
  Fun.protect ~finally:(fun () -> ignore (Sqlite3.finalize stmt)) (fun () ->
    match Sqlite3.step stmt with
    | Sqlite3.Rc.ROW ->
        (match Sqlite3.column stmt 0 with
         | Sqlite3.Data.INT n -> Int64.to_int n
         | _ -> -1)
    | _ -> -1)

(* --- Tests --- *)

let test_store_smoke =
  Testo.create "store.content_put_get_round_trip" (fun () ->
    with_in_memory (fun _ ->
      let sha = SerDe.sha256 (Bytes.of_string "hello") in
      Store.content_put sha (Bytes.of_string "hello");
      (match Store.content_get sha with
       | Some b when Bytes.to_string b = "hello" -> ()
       | Some b -> Testo.fail (Printf.sprintf "unexpected: %S" (Bytes.to_string b))
       | None -> Testo.fail "missing content")))

let test_merkle_direct =
  Testo.create "serde.merkle_round_trip" (fun () ->
    with_in_memory (fun _ ->
      let sha_a = SerDe.int.store 7 in
      let sha_b = SerDe.int.store 11 in
      let parent = SerDe.Merkle.store ~tag:42 [sha_a; sha_b] in
      match SerDe.Merkle.load parent with
      | Some (42, [ca; cb]) when Bytes.equal ca sha_a && Bytes.equal cb sha_b -> ()
      | Some (t, cs) ->
          Testo.fail (Printf.sprintf "tag=%d, children=%d" t (List.length cs))
      | None -> Testo.fail "merkle load returned None"))

let test_schema_only_content =
  Testo.create "store.schema_only_content_table" (fun () ->
    with_temp_file_db (fun path uri ->
      Store.with_cdn uri (fun t ->
        Store.with_handlers t (fun _ ->
          Store.content_put
            (SerDe.sha256 (Bytes.of_string "x"))
            (Bytes.of_string "x")));
      let db = Sqlite3.db_open path in
      Fun.protect ~finally:(fun () -> ignore (Sqlite3.db_close db)) (fun () ->
        let tables = ref [] in
        let stmt = Sqlite3.prepare db
          "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        in
        Fun.protect ~finally:(fun () -> ignore (Sqlite3.finalize stmt)) (fun () ->
          let rec loop () =
            match Sqlite3.step stmt with
            | Sqlite3.Rc.ROW ->
                (match Sqlite3.column stmt 0 with
                 | Sqlite3.Data.TEXT n -> tables := n :: !tables
                 | _ -> ());
                loop ()
            | _ -> ()
          in
          loop ());
        let names = List.sort String.compare !tables in
        if List.mem "merkle_info" names then
          Testo.fail "merkle_info table should not exist";
        if not (List.mem "content" names) then
          Testo.fail "content table missing")))

let test_content_dedup =
  Testo.create "store.content_dedup" (fun () ->
    with_temp_file_db (fun path uri ->
      Store.with_cdn uri (fun t ->
        Store.with_handlers t (fun _ ->
          let _ = SerDe.int.store 42 in
          let _ = SerDe.int.store 42 in
          let _ = SerDe.int.store 42 in
          ()));
      let db = Sqlite3.db_open path in
      Fun.protect ~finally:(fun () -> ignore (Sqlite3.db_close db)) (fun () ->
        let n = count_rows db "SELECT count(*) FROM content" in
        if n <> 1 then Testo.fail (Printf.sprintf "expected 1 content row, got %d" n))))

let test_list_shared_elements_dedup =
  Testo.create "serde.list_dedups_shared_elements" (fun () ->
    with_temp_file_db (fun path uri ->
      let sha1_ref = ref Bytes.empty in
      let sha2_ref = ref Bytes.empty in
      Store.with_cdn uri (fun t ->
        Store.with_handlers t (fun _ ->
          let serde = SerDe.list SerDe.int in
          sha1_ref := serde.store [ 1; 2; 3 ];
          sha2_ref := serde.store [ 4; 2; 5 ]));
      (* Expected content rows:
           - 5 distinct ints: {1, 2, 3, 4, 5} — the [2] is shared
           - 2 distinct list merkle blobs (different orderings)
         Total: 7. *)
      let db = Sqlite3.db_open path in
      Fun.protect ~finally:(fun () -> ignore (Sqlite3.db_close db)) (fun () ->
        let n = count_rows db "SELECT count(*) FROM content" in
        if n <> 7 then
          Testo.fail
            (Printf.sprintf
               "expected 7 content rows (5 ints + 2 list blobs), got %d" n));
      (* Different list order → different root SHAs. *)
      if Bytes.equal !sha1_ref !sha2_ref then
        Testo.fail "list root SHAs should differ for different orderings"))

let test_identical_lists_share_sha =
  Testo.create "serde.identical_composites_share_sha" (fun () ->
    with_temp_file_db (fun path uri ->
      let sha1_ref = ref Bytes.empty in
      let sha2_ref = ref Bytes.empty in
      Store.with_cdn uri (fun t ->
        Store.with_handlers t (fun _ ->
          let serde = SerDe.list SerDe.int in
          sha1_ref := serde.store [ 10; 20; 30 ];
          sha2_ref := serde.store [ 10; 20; 30 ]));
      if not (Bytes.equal !sha1_ref !sha2_ref) then
        Testo.fail "identical composites should produce identical root SHAs";
      (* 3 ints + 1 merkle blob = 4 content rows, not 8. *)
      let db = Sqlite3.db_open path in
      Fun.protect ~finally:(fun () -> ignore (Sqlite3.db_close db)) (fun () ->
        let n = count_rows db "SELECT count(*) FROM content" in
        if n <> 4 then
          Testo.fail
            (Printf.sprintf
               "expected 4 content rows (3 ints + 1 list blob), got %d" n))))

let test_nested_composite_dedup =
  Testo.create "serde.nested_composite_structural_sharing" (fun () ->
    with_temp_file_db (fun path uri ->
      Store.with_cdn uri (fun t ->
        Store.with_handlers t (fun _ ->
          let int_list = SerDe.list SerDe.int in
          let outer = SerDe.pair int_list int_list in
          let shared = [ 1; 2; 3 ] in
          let _ = outer.store (shared, shared) in
          ()));
      (* Content tally:
           - 3 ints: {1, 2, 3}
           - 1 inner list blob for [1;2;3] (stored once even though
             referenced twice)
           - 1 outer pair blob whose two children point at the same
             inner blob sha
         Total: 5 rows. *)
      let db = Sqlite3.db_open path in
      Fun.protect ~finally:(fun () -> ignore (Sqlite3.db_close db)) (fun () ->
        let n = count_rows db "SELECT count(*) FROM content" in
        if n <> 5 then
          Testo.fail
            (Printf.sprintf
               "expected 5 content rows (3 ints + inner list + outer pair), \
                got %d" n))))

let test_serde_list =
  Testo.create "serde.list_round_trip" (fun () ->
    with_in_memory (fun _ ->
      let serde = SerDe.list SerDe.int in
      let sha = serde.store [1; 2; 3; 4; 5] in
      let xs = serde.load sha in
      if xs <> [1; 2; 3; 4; 5] then
        Testo.fail (Printf.sprintf "got %s"
                      (String.concat "," (List.map string_of_int xs)))))

let test_serde_option =
  Testo.create "serde.option_round_trip" (fun () ->
    with_in_memory (fun _ ->
      let serde = SerDe.option SerDe.string in
      let sha_n = serde.store None in
      let sha_s = serde.store (Some "hi") in
      (match serde.load sha_n with
       | None -> ()
       | Some _ -> Testo.fail "expected None");
      (match serde.load sha_s with
       | Some "hi" -> ()
       | _ -> Testo.fail "expected Some hi")))

let test_serde_result =
  Testo.create "serde.result_round_trip" (fun () ->
    with_in_memory (fun _ ->
      let serde = SerDe.result SerDe.int SerDe.string in
      let sha_ok = serde.store (Ok 42) in
      let sha_err = serde.store (Error "nope") in
      (match serde.load sha_ok with
       | Ok 42 -> ()
       | _ -> Testo.fail "expected Ok 42");
      (match serde.load sha_err with
       | Error "nope" -> ()
       | _ -> Testo.fail "expected Error nope")))

let test_serde_ref =
  Testo.create "serde.ref_round_trip_no_aliasing" (fun () ->
    with_in_memory (fun _ ->
      let serde = SerDe.ref SerDe.int in
      let r = ref 7 in
      let sha = serde.store r in
      let r' = serde.load sha in
      if !r' <> 7 then Testo.fail "expected !r' = 7";
      (* Mutate the original — loaded ref must not observe the change. *)
      r := 999;
      if !r' <> 7 then
        Testo.fail "aliasing leaked: loaded ref saw mutation of source";
      (* Mutate the loaded ref — source must not observe it. *)
      r' := 123;
      if !r <> 999 then
        Testo.fail "aliasing leaked: source saw mutation of loaded ref"))

let test_serde_pair =
  Testo.create "serde.pair_round_trip" (fun () ->
    with_in_memory (fun _ ->
      let serde = SerDe.pair SerDe.int SerDe.string in
      let sha = serde.store (7, "world") in
      match serde.load sha with
      | (7, "world") -> ()
      | _ -> Testo.fail "pair mismatch"))

module My_cache : Cache.S with type key = int and type value = string = struct
  let table_name = "test_my_cache"
  type key = int
  type value = string
  let key_serde   = SerDe.int
  let value_serde = SerDe.string
end

module C = Cache.Make (My_cache)

let test_cache_round_trip =
  Testo.create "cache.put_get_delete" (fun () ->
    with_in_memory (fun _ ->
      C.put 1 "one";
      C.put 2 "two";
      (match C.get_opt 1 with
       | Some "one" -> ()
       | _ -> Testo.fail "expected Some \"one\"");
      (match C.get_opt 2 with
       | Some "two" -> ()
       | _ -> Testo.fail "expected Some \"two\"");
      (match C.get_opt 99 with
       | None -> ()
       | Some _ -> Testo.fail "expected None for missing key");
      (if not (C.delete 1) then Testo.fail "delete 1 returned false");
      (match C.get_opt 1 with
       | None -> ()
       | Some _ -> Testo.fail "expected None after delete");
      (if C.delete 1 then Testo.fail "second delete 1 should return false")))

module Memoed = Memo.Make (My_cache)

let body_count = ref 0
let expensive_body n = incr body_count; "n=" ^ string_of_int n

let test_memo_hit_miss =
  Testo.create "memo.memoize1_hit_miss" (fun () ->
    with_in_memory (fun _ ->
      body_count := 0;
      let f = Memoed.memoize1 ~key:(fun n -> n) ~body:expensive_body in
      let r1 = f 5 in
      let r2 = f 5 in
      let r3 = f 6 in
      let r4 = f 5 in
      if r1 <> "n=5" then Testo.fail "r1";
      if r2 <> "n=5" then Testo.fail "r2";
      if r3 <> "n=6" then Testo.fail "r3";
      if r4 <> "n=5" then Testo.fail "r4";
      if !body_count <> 2 then
        Testo.fail (Printf.sprintf "body ran %d times, expected 2" !body_count)))

let test_memo_keyed1 =
  Testo.create "memo.memoize2_keyed1" (fun () ->
    with_in_memory (fun _ ->
      body_count := 0;
      let body n _pass = incr body_count; "n=" ^ string_of_int n in
      let f = Memoed.memoize2_keyed1 ~key:(fun n -> n) ~body in
      let _ = f 3 "a" in
      let _ = f 3 "b" in
      let _ = f 3 "c" in
      let _ = f 4 "x" in
      if !body_count <> 2 then
        Testo.fail (Printf.sprintf "body ran %d times, expected 2" !body_count)))

let test_multi_domain =
  Testo.create "store.multi_domain_visibility" (fun () ->
    with_temp_file_db (fun _ uri ->
      Store.with_cdn uri (fun t ->
        let sha1 = Atomic.make Bytes.empty in
        let sha2 = Atomic.make Bytes.empty in
        let d1 = Domain.spawn (fun () ->
          Store.with_handlers t (fun _ ->
            Atomic.set sha1 (SerDe.int.store 111)))
        in
        let d2 = Domain.spawn (fun () ->
          Store.with_handlers t (fun _ ->
            Atomic.set sha2 (SerDe.int.store 222)))
        in
        Domain.join d1;
        Domain.join d2;
        Store.with_handlers t (fun _ ->
          let v1 = SerDe.int.load (Atomic.get sha1) in
          let v2 = SerDe.int.load (Atomic.get sha2) in
          if v1 <> 111 then Testo.fail (Printf.sprintf "v1=%d" v1);
          if v2 <> 222 then Testo.fail (Printf.sprintf "v2=%d" v2)))))

let test_persistence_across_reopen =
  Testo.create "store.file_persists_across_reopen" (fun () ->
    with_temp_file_db (fun _ uri ->
      let module MC : Cache.S with type key = string and type value = int = struct
        let table_name = "persist_cache"
        type key = string
        type value = int
        let key_serde   = SerDe.string
        let value_serde = SerDe.int
      end in
      let module C = Cache.Make (MC) in
      Store.with_cdn uri (fun t ->
        Store.with_handlers t (fun _ ->
          C.put "hello" 42));
      Store.with_cdn uri (fun t ->
        Store.with_handlers t (fun _ ->
          match C.get_opt "hello" with
          | Some 42 -> ()
          | Some n -> Testo.fail (Printf.sprintf "got %d" n)
          | None -> Testo.fail "missing after reopen"))))

let tests =
  [ test_store_smoke
  ; test_merkle_direct
  ; test_schema_only_content
  ; test_content_dedup
  ; test_list_shared_elements_dedup
  ; test_identical_lists_share_sha
  ; test_nested_composite_dedup
  ; test_serde_list
  ; test_serde_option
  ; test_serde_result
  ; test_serde_ref
  ; test_serde_pair
  ; test_cache_round_trip
  ; test_memo_hit_miss
  ; test_memo_keyed1
  ; test_multi_domain
  ; test_persistence_across_reopen
  ]

let () = Testo.interpret_argv ~project_name:"camldn" (fun _env -> tests)
