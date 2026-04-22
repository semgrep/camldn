# CAMLDN — claude notes

## What this is

A SQLite-backed, content-addressed cache library for OCaml 5.3+. Two packages:
- `camldn` (runtime) — `Store`, `SerDe`, `Cache`, `Memo`.
- `ppx_camldn` (PPX) — `[@@deriving camldn]` and `let%memo`.

See `plan.md` for the full design; `/Users/r2cuser/.claude/plans/this-is-camldn-a-fluffy-quokka.md`
has the long-form reasoning.

## Architecture cheat-sheet

Two shared tables + one per named cache:

- `content (sha BLOB PK, content BLOB)` — every serialized value, SHA-256 keyed.
- `<cache_name> (key_sha PK, value_sha)` — per user-defined cache.

Primitives go through `bin_prot` and land in `content` directly. Composites
(option/result/list/tuples/user variants/user records) get a merkle blob:

```
[ 0x01 (version) | tag (u16 LE) | n (u32 LE) | sha_1 (32B) | … | sha_n (32B) ]
```

…stored in `content`. The blob itself contains the child SHAs in order, so no
separate edge table is needed — `load` just parses the blob and dereferences.

## Effects

Everything CRUD-ish flows through algebraic effects declared in `Store`:

```
Content_put / Content_get
Ensure_cache
Cache_put / Cache_get / Cache_delete
```

`Store.with_handlers t f` installs the default sqlite-backed handler for the
scope of `f`. `Domain.spawn` children must re-install handlers themselves.

## Module responsibilities

- **`Store`** — no ADTs beyond `type t` + effect declarations. Pure bytes in/out.
- **`SerDe`** — `type 'a t = { store : 'a -> bytes; load : bytes -> 'a }`.
- **`Cache`** — `module type S` with `key_serde`/`value_serde`, and `Make`.
- **`Memo`** — arity-indexed combinators. Cap at 4; higher arities uncurry.

## Conventions

- No GADTs anywhere except `type _ Effect.t +=` — effects need it, everything
  else stays concrete for predictability.
- No `col_val`/`schema`/`row` types. If you find yourself reaching for one,
  stop — bytes + merkle blobs are the only shapes.
- `Cache.Make` calls `ensure_table` at every op entry point (no-op after first
  call, memoized in `Store`'s hashtable). Callers don't need to remember.

## PPX

- `[@@deriving camldn]` on records and variants generates
  `serde_<t> : <t> Camldn.SerDe.t`. Mutually recursive type groups get one
  `let rec`/`and`.
- Two interchangeable memo syntaxes, both support `[@cache M]` (required,
  names the `Cache.S` module) and `[@key arg_names…]` (optional, default is
  all args):

    ```ocaml
    (* extension syntax *)
    let%memo[@cache M][@key a b] f a b c = ...

    (* trailing-attribute syntax — attributes attach to the body *)
    let f a b c = ... [@cache M][@key a b]
    ```

  The attribute rewriter walks `Pexp_fun`/`Pexp_constraint`/`Pexp_newtype`/
  `Pexp_sequence` wrappers, promotes any `[@cache]`/`[@key]` to the binding,
  then reuses the same expansion as `let%memo`.

## Build / test workflow

```
make build   # nix develop --no-pure-eval -c dune build
make test    # nix develop --no-pure-eval -c dune runtest
make shell   # interactive nix dev shell
```

After editing `dune-project` (which regenerates `.opam` files), run
`direnv reload` so the nix-built dev shell picks up any new deps.

## SHA

SHA-256 via `digestif` (`Digestif.SHA256`). 32-byte digests, stored as BLOB
in sqlite.

## Serialization

`bin_prot` for primitive leaves. `SerDe.of_bin_prot` wraps a
`'a Bin_prot.Type_class.t` into a `'a SerDe.t` that stores the bin_prot bytes
directly. Composites never call bin_prot on themselves — they only bin_prot
their leaves and then merkle-compose.

## Mutability, cycles, aliasing

Content-addressing is inherently a pure-value DAG model, so:

- **Cyclic values** (`let rec l = 1 :: l`, mutable self-pointers) will stack-
  overflow at `store` time. SHAs of a cycle would have to reference themselves,
  which has no fixed point. Cache values must be acyclic.
- **`'a ref`** is supported via `SerDe.ref` and `[@@deriving camldn]` on record
  fields of type `'a ref`. The ref is merkle-wrapped (`tag=0`, one child) and
  `load` returns a *fresh* `ref` each call. **Aliasing is not preserved** — if
  two refs in your source data point at the same cell, they collapse to one
  blob on write but come back as two independent cells on read. Same for
  mutable record fields under any future deriver.
- **Sharing by value** (distinct-but-equal subtrees) *is* preserved: content-
  addressing collapses them automatically. This is stronger than `Marshal`'s
  by-identity sharing for cross-run dedup, but weaker than `Marshal` on
  in-memory cycle/aliasing fidelity.

## Known limitations

- 64-bit platforms only (`int` via `bin_int`, 63-bit tagged).
- No TTL / eviction.
- Per-Domain conn leaks if the Domain exits before `close_cdn`.
- No cycle / aliasing preservation (see previous section).
