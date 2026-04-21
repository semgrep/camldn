module Make (C : Cache.S) = struct
  module C_ops = Cache.Make (C)

  let go k body_thunk =
    match C_ops.get_opt k with
    | Some v -> v
    | None ->
        let v = body_thunk () in
        C_ops.put k v;
        v

  let memoize1 ~key ~body a           = go (key a)       (fun () -> body a)
  let memoize2 ~key ~body a b         = go (key a b)     (fun () -> body a b)
  let memoize3 ~key ~body a b c       = go (key a b c)   (fun () -> body a b c)
  let memoize4 ~key ~body a b c d     = go (key a b c d) (fun () -> body a b c d)

  let memoize2_keyed1 ~key ~body a b       = go (key a)     (fun () -> body a b)
  let memoize3_keyed1 ~key ~body a b c     = go (key a)     (fun () -> body a b c)
  let memoize3_keyed2 ~key ~body a b c     = go (key a b)   (fun () -> body a b c)
  let memoize4_keyed1 ~key ~body a b c d   = go (key a)     (fun () -> body a b c d)
  let memoize4_keyed2 ~key ~body a b c d   = go (key a b)   (fun () -> body a b c d)
  let memoize4_keyed3 ~key ~body a b c d   = go (key a b c) (fun () -> body a b c d)
end
