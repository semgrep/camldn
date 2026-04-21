(** Memoize OCaml functions against a [Cache.S].

    All arguments participate in the cache key unless you use a [_keyedM]
    variant, in which case the first M arguments are keyed and the remaining
    pass through untouched. *)

module Make (C : Cache.S) : sig
  val memoize1 :
    key:('a -> C.key) ->
    body:('a -> C.value) ->
    ('a -> C.value)

  val memoize2 :
    key:('a -> 'b -> C.key) ->
    body:('a -> 'b -> C.value) ->
    ('a -> 'b -> C.value)

  val memoize3 :
    key:('a -> 'b -> 'c -> C.key) ->
    body:('a -> 'b -> 'c -> C.value) ->
    ('a -> 'b -> 'c -> C.value)

  val memoize4 :
    key:('a -> 'b -> 'c -> 'd -> C.key) ->
    body:('a -> 'b -> 'c -> 'd -> C.value) ->
    ('a -> 'b -> 'c -> 'd -> C.value)

  val memoize2_keyed1 :
    key:('a -> C.key) ->
    body:('a -> 'b -> C.value) ->
    ('a -> 'b -> C.value)

  val memoize3_keyed1 :
    key:('a -> C.key) ->
    body:('a -> 'b -> 'c -> C.value) ->
    ('a -> 'b -> 'c -> C.value)

  val memoize3_keyed2 :
    key:('a -> 'b -> C.key) ->
    body:('a -> 'b -> 'c -> C.value) ->
    ('a -> 'b -> 'c -> C.value)

  val memoize4_keyed1 :
    key:('a -> C.key) ->
    body:('a -> 'b -> 'c -> 'd -> C.value) ->
    ('a -> 'b -> 'c -> 'd -> C.value)

  val memoize4_keyed2 :
    key:('a -> 'b -> C.key) ->
    body:('a -> 'b -> 'c -> 'd -> C.value) ->
    ('a -> 'b -> 'c -> 'd -> C.value)

  val memoize4_keyed3 :
    key:('a -> 'b -> 'c -> C.key) ->
    body:('a -> 'b -> 'c -> 'd -> C.value) ->
    ('a -> 'b -> 'c -> 'd -> C.value)
end
