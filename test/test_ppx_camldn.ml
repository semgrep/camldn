(** Tests for ppx_camldn — verifies the [@@cache M] attribute generates
    correct caching wrappers at runtime. *)

(* ---- test cache modules ---- *)

module StringIntCache =
  Camldn.MakeMarshal (struct
    type param = string
    type value = int
    let key_of_param s = s
    let closures = false
  end)

module StringStringCache =
  Camldn.MakeMarshal (struct
    type param = string
    type value = string
    let key_of_param s = s
    let closures = false
  end)

(* ---- helpers ---- *)

let with_test_cdn f =
  Camldn.with_cdn ~in_memory:true (Uri.of_string "file:///test.db") (fun cdn ->
      Camldn.with_handlers cdn (fun _cdn -> f ()))

(* ---- functions under test ---- *)

let call_count = ref 0

let expensive_length (s : string) : int =
  incr call_count;
  String.length s
[@@cache StringIntCache]

let no_annot_count = ref 0

let expensive_upper (s : string) : string =
  incr no_annot_count;
  String.uppercase_ascii s
[@@cache StringStringCache]

let plain_param_count = ref 0

let plain_param s : int =
  incr plain_param_count;
  String.length s
[@@cache StringIntCache]

module Inner = struct
  let inner_count = ref 0

  let inner_fn (s : string) : int =
    incr inner_count;
    String.length s
  [@@cache StringIntCache]
end

(* ---- tests ---- *)

let test_basic_caching () =
  with_test_cdn (fun () ->
      call_count := 0;
      let r1 = expensive_length "hello" in
      let r2 = expensive_length "hello" in
      assert (r1 = 5);
      assert (r2 = 5);
      assert (!call_count = 1))

let test_different_params () =
  with_test_cdn (fun () ->
      call_count := 0;
      let r1 = expensive_length "foo" in
      let r2 = expensive_length "bar" in
      assert (r1 = 3);
      assert (r2 = 3);
      assert (!call_count = 2))

let test_different_cache_module () =
  with_test_cdn (fun () ->
      no_annot_count := 0;
      let r1 = expensive_upper "hello" in
      let r2 = expensive_upper "hello" in
      assert (r1 = "HELLO");
      assert (r2 = "HELLO");
      assert (!no_annot_count = 1))

let test_plain_param () =
  with_test_cdn (fun () ->
      plain_param_count := 0;
      let r1 = plain_param "test" in
      let r2 = plain_param "test" in
      assert (r1 = 4);
      assert (r2 = 4);
      assert (!plain_param_count = 1))

let test_nested_module () =
  with_test_cdn (fun () ->
      Inner.inner_count := 0;
      let r1 = Inner.inner_fn "abc" in
      let r2 = Inner.inner_fn "abc" in
      assert (r1 = 3);
      assert (r2 = 3);
      assert (!Inner.inner_count = 1))

let tests =
  [ Testo.create "basic caching" test_basic_caching
  ; Testo.create "different params" test_different_params
  ; Testo.create "different cache module" test_different_cache_module
  ; Testo.create "plain param (no type annotation)" test_plain_param
  ; Testo.create "nested module qualified path" test_nested_module
  ]

let () = Testo.interpret_argv ~project_name:"ppx_camldn" (fun _env -> tests)
