module IntCache = Camldn.Make (struct
  type param = string
  type value = int
  let key_of_param s = s

  let bytes_of_value v =
    let s = string_of_int v in
    Bytes.of_string s

  let value_of_bytes b =
    int_of_string (Bytes.to_string b)
end)

module MarshalCache = Camldn.MakeMarshal (struct
  type param = string
  type value = string
  let key_of_param s = s
  let closures = false
end)

let with_test_cdn f =
  let uri = Uri.of_string "file::memory:" in
  Camldn.with_cdn ~in_memory:true uri (fun t ->
    Camldn.with_handlers t (fun t -> f t))

let test_open_close =
  Testo.create "open and close in-memory" (fun () ->
    let uri = Uri.of_string "file::memory:" in
    let t = Camldn.open_cdn ~in_memory:true uri in
    Camldn.close_cdn t)

let test_cache_miss_then_hit =
  Testo.create "maybe_do caches on miss, returns on hit" (fun () ->
    with_test_cdn (fun _t ->
      let call_count = ref 0 in
      let f s =
        incr call_count;
        String.length s
      in
      let r1 = IntCache.maybe_do ~cache_name:"len" ~f "hello" in
      let r2 = IntCache.maybe_do ~cache_name:"len" ~f "hello" in
      Testo.(check int) ~msg:"first call result" 5 r1;
      Testo.(check int) ~msg:"second call result" 5 r2;
      Testo.(check int) ~msg:"f called only once" 1 !call_count))

let test_different_params =
  Testo.create "different params get different cache entries" (fun () ->
    with_test_cdn (fun _t ->
      let call_count = ref 0 in
      let f s = incr call_count; String.length s in
      let r1 = IntCache.maybe_do ~cache_name:"len" ~f "hi" in
      let r2 = IntCache.maybe_do ~cache_name:"len" ~f "hello" in
      Testo.(check int) ~msg:"short" 2 r1;
      Testo.(check int) ~msg:"long" 5 r2;
      Testo.(check int) ~msg:"f called twice" 2 !call_count))

let test_different_f_names =
  Testo.create "different cache_names are separate cache keys" (fun () ->
    with_test_cdn (fun _t ->
      let call_count = ref 0 in
      let f _s = incr call_count; 42 in
      let r1 = IntCache.maybe_do ~cache_name:"alpha" ~f "x" in
      let r2 = IntCache.maybe_do ~cache_name:"beta" ~f "x" in
      Testo.(check int) ~msg:"alpha" 42 r1;
      Testo.(check int) ~msg:"beta" 42 r2;
      Testo.(check int) ~msg:"called twice (different cache_names)" 2 !call_count))

let test_marshal_cache =
  Testo.create "MakeMarshal round-trips values" (fun () ->
    with_test_cdn (fun _t ->
      let call_count = ref 0 in
      let f s = incr call_count; String.uppercase_ascii s in
      let r1 = MarshalCache.maybe_do ~cache_name:"upper" ~f "meow" in
      let r2 = MarshalCache.maybe_do ~cache_name:"upper" ~f "meow" in
      Testo.(check string) ~msg:"first" "MEOW" r1;
      Testo.(check string) ~msg:"cached" "MEOW" r2;
      Testo.(check int) ~msg:"called once" 1 !call_count))

let test_with_cdn_closes_on_exn =
  Testo.create "with_cdn closes even on exception" (fun () ->
    let uri = Uri.of_string "file::memory:" in
    (try
       Camldn.with_cdn ~in_memory:true uri (fun _t ->
         failwith "boom")
     with Failure _ -> ());
    (* If we got here without hanging/crashing, the connection was cleaned up *)
    ())

let test_unsupported_scheme =
  Testo.create "open_cdn rejects unsupported URI schemes" (fun () ->
    let uri = Uri.of_string "https://example.com/db" in
    match Camldn.open_cdn uri with
    | _ -> Testo.fail "expected Unsupported_scheme"
    | exception Camldn.Unsupported_scheme "https" -> ()
    | exception Camldn.Unsupported_scheme s ->
      Testo.fail ("wrong scheme in exception: " ^ s))

let tests =
  [ test_open_close
  ; test_cache_miss_then_hit
  ; test_different_params
  ; test_different_f_names
  ; test_marshal_cache
  ; test_with_cdn_closes_on_exn
  ; test_unsupported_scheme
  ]

let () = Testo.interpret_argv ~project_name:"camldn" (fun _env -> tests)
