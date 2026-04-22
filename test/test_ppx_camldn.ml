module Store = Camldn.Store
module SerDe = Camldn.SerDe
module Cache = Camldn.Cache
module Memo  = Camldn.Memo

type user = {
  id      : int;
  name    : string;
  friends : string list;
} [@@deriving camldn]

type status =
  | Active
  | Inactive of string
  | Banned   of string * int
  [@@deriving camldn]

type tree =
  | Leaf
  | Node of tree * int * tree
  [@@deriving camldn]

let with_in_memory f =
  Store.with_cdn ~in_memory:true (Uri.of_string "") (fun t ->
    Store.with_handlers t (fun _ -> f ()))

let with_temp_file_db f =
  let path = Filename.temp_file "camldn_ppx_test_" ".db" in
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

(* --- Deriving tests --- *)

module User_cache : Cache.S with type key = string and type value = user = struct
  let table_name  = "user_by_name"
  type key   = string
  type value = user
  let key_serde   = SerDe.string
  let value_serde = serde_user
end

module UC = Cache.Make (User_cache)

let test_record_round_trip =
  Testo.create "ppx.derive_record_round_trip" (fun () ->
    with_in_memory (fun () ->
      let u = { id = 42; name = "ada"; friends = ["alan"; "grace"] } in
      UC.put "ada" u;
      match UC.get_opt "ada" with
      | Some u' when u' = u -> ()
      | Some u' ->
          Testo.fail
            (Printf.sprintf "roundtrip mismatch: id=%d name=%s" u'.id u'.name)
      | None -> Testo.fail "missing after put"))

module Status_cache : Cache.S with type key = int and type value = status = struct
  let table_name  = "status_by_id"
  type key   = int
  type value = status
  let key_serde   = SerDe.int
  let value_serde = serde_status
end

module SC = Cache.Make (Status_cache)

let test_variant_round_trip =
  Testo.create "ppx.derive_variant_round_trip" (fun () ->
    with_in_memory (fun () ->
      SC.put 1 Active;
      SC.put 2 (Inactive "sleeping");
      SC.put 3 (Banned ("spam", 30));
      (match SC.get_opt 1 with
       | Some Active -> ()
       | _ -> Testo.fail "Active");
      (match SC.get_opt 2 with
       | Some (Inactive "sleeping") -> ()
       | _ -> Testo.fail "Inactive");
      (match SC.get_opt 3 with
       | Some (Banned ("spam", 30)) -> ()
       | _ -> Testo.fail "Banned")))

module Tree_cache : Cache.S with type key = string and type value = tree = struct
  let table_name  = "trees"
  type key   = string
  type value = tree
  let key_serde   = SerDe.string
  let value_serde = serde_tree
end

module TC = Cache.Make (Tree_cache)

let test_recursive_round_trip =
  Testo.create "ppx.derive_recursive_round_trip" (fun () ->
    with_in_memory (fun () ->
      let t =
        Node (Node (Leaf, 1, Leaf), 2, Node (Leaf, 3, Node (Leaf, 4, Leaf)))
      in
      TC.put "mytree" t;
      match TC.get_opt "mytree" with
      | Some t' when t' = t -> ()
      | _ -> Testo.fail "recursive round-trip failed"))

(* --- let%memo tests --- *)

module Count_cache : Cache.S with type key = int and type value = int = struct
  let table_name  = "count_cache"
  type key   = int
  type value = int
  let key_serde   = SerDe.int
  let value_serde = SerDe.int
end

let memo_count = ref 0

let%memo[@cache Count_cache] square (n : int) : int =
  incr memo_count;
  n * n

let test_memo_basic =
  Testo.create "ppx.let_memo_basic" (fun () ->
    with_in_memory (fun () ->
      memo_count := 0;
      let _ = square 3 in
      let _ = square 3 in
      let _ = square 3 in
      let _ = square 4 in
      let _ = square 3 in
      if !memo_count <> 2 then
        Testo.fail
          (Printf.sprintf "body ran %d times, expected 2" !memo_count)))

module Parse_cache : Cache.S with type key = string and type value = int = struct
  let table_name  = "parse_cache"
  type key   = string
  type value = int
  let key_serde   = SerDe.string
  let value_serde = SerDe.int
end

let parse_count = ref 0

let%memo[@cache Parse_cache][@key src]
  parse_with_opts (src : string) (strict : bool) : int =
  incr parse_count;
  String.length src + (if strict then 1000 else 0)

let test_memo_partial_key =
  Testo.create "ppx.let_memo_partial_key" (fun () ->
    with_in_memory (fun () ->
      parse_count := 0;
      let _ = parse_with_opts "hello" true in
      let _ = parse_with_opts "hello" false in
      let _ = parse_with_opts "hello" true in
      let _ = parse_with_opts "world" false in
      let _ = parse_with_opts "world" true in
      if !parse_count <> 2 then
        Testo.fail
          (Printf.sprintf "body ran %d times, expected 2" !parse_count)))

(* Same as above but with the trailing-attribute syntax (no let%memo): *)
let attr_parse_count = ref 0

let parse_attr_style (src : string) (strict : bool) : int =
  incr attr_parse_count;
  String.length src + (if strict then 1000 else 0)
  [@cache Parse_cache][@key src]

let test_memo_attr_style =
  Testo.create "ppx.let_memo_attribute_style" (fun () ->
    with_in_memory (fun () ->
      attr_parse_count := 0;
      let _ = parse_attr_style "hi"   true  in
      let _ = parse_attr_style "hi"   false in
      let _ = parse_attr_style "hi"   true  in
      let _ = parse_attr_style "yo"   false in
      let _ = parse_attr_style "yo"   true  in
      if !attr_parse_count <> 2 then
        Testo.fail
          (Printf.sprintf "body ran %d times, expected 2" !attr_parse_count)))

(* Structural-sharing check through a derived record. Two separate record
   values share a sub-record, which should collapse to a single blob. *)

type inner = { inner_name : string; inner_n : int } [@@deriving camldn]
type outer = { first : inner; second : inner } [@@deriving camldn]

type counter = { label : string; count : int ref } [@@deriving camldn]

type boxed = { name : string; value : int lazy_t } [@@deriving camldn]

let test_ppx_lazy_field =
  Testo.create "ppx.record_with_lazy_field" (fun () ->
    with_in_memory (fun () ->
      let force_count = ref 0 in
      let b = { name = "box"; value = lazy (incr force_count; 42) } in
      if !force_count <> 0 then Testo.fail "forced too early";
      let sha = serde_boxed.store b in
      if !force_count <> 1 then
        Testo.fail
          (Printf.sprintf "expected 1 force during store, got %d" !force_count);
      let b' = serde_boxed.load sha in
      if b'.name <> "box" then Testo.fail "name lost";
      if not (Lazy.is_val b'.value) then
        Testo.fail "loaded lazy field not pre-forced";
      if Lazy.force b'.value <> 42 then Testo.fail "wrong value";
      if !force_count <> 1 then
        Testo.fail "loading re-ran the thunk"))

let test_ppx_ref_field =
  Testo.create "ppx.record_with_ref_field" (fun () ->
    with_in_memory (fun () ->
      let c = { label = "hits"; count = ref 41 } in
      let sha = serde_counter.store c in
      let c' = serde_counter.load sha in
      if c'.label <> "hits" then Testo.fail "label lost";
      if !(c'.count) <> 41 then Testo.fail "count lost";
      (* Aliasing not preserved: mutating source doesn't affect the loaded copy. *)
      c.count := 999;
      if !(c'.count) <> 41 then
        Testo.fail "aliasing leaked via deriving"))

let test_ppx_record_structural_sharing =
  Testo.create "ppx.record_shared_subtree_dedup" (fun () ->
    with_temp_file_db (fun path uri ->
      Store.with_cdn uri (fun t ->
        Store.with_handlers t (fun _ ->
          let i = { inner_name = "shared"; inner_n = 99 } in
          let o = { first = i; second = i } in
          let _ = serde_outer.store o in
          ()));
      (* Expected content rows:
         - "shared" string blob (1)
         - 99 int blob              (1)
         - inner merkle blob (1 — stored once, referenced by first AND second)
         - outer merkle blob (1, whose two children point at the SAME inner sha)
         Total: 4. *)
      let db = Sqlite3.db_open path in
      Fun.protect ~finally:(fun () -> ignore (Sqlite3.db_close db)) (fun () ->
        let n = count_rows db "SELECT count(*) FROM content" in
        if n <> 4 then
          Testo.fail
            (Printf.sprintf
               "expected 4 content rows (1 string + 1 int + 1 inner blob \
                + 1 outer blob), got %d" n))))

let tests =
  [ test_record_round_trip
  ; test_variant_round_trip
  ; test_recursive_round_trip
  ; test_memo_basic
  ; test_memo_partial_key
  ; test_memo_attr_style
  ; test_ppx_record_structural_sharing
  ; test_ppx_ref_field
  ; test_ppx_lazy_field
  ]

let () = Testo.interpret_argv ~project_name:"ppx_camldn" (fun _env -> tests)
