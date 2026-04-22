# CAMLDN Implementation Plan

A SQLite-backed, content-addressed cache library for OCaml 5.3+, built on algebraic
effect handlers.

## Architecture

Two shared SQLite tables + one per named cache:

```sql
CREATE TABLE content (
  sha     BLOB PRIMARY KEY NOT NULL,
  content BLOB NOT NULL
) WITHOUT ROWID;

CREATE TABLE <cache_name> (
  key_sha   BLOB PRIMARY KEY NOT NULL,
  value_sha BLOB NOT NULL,
  FOREIGN KEY (key_sha)   REFERENCES content(sha),
  FOREIGN KEY (value_sha) REFERENCES content(sha)
) WITHOUT ROWID;
```

### Encoding

- **Primitives** (`int`, `int64`, `float`, `bool`, `string`, `bytes`, `unit`):
  bin_prot bytes → SHA-256 → `INSERT OR IGNORE INTO content`.
- **Composites** (polymorphic ADTs, variants, records): canonical merkle blob
  ```
  [ 0x01 (1B ver) | tag (2B LE) | n (4B LE) | sha_1 (32B) | … | sha_n (32B) ]
  ```
  → SHA-256 → `INSERT OR IGNORE INTO content`. The blob itself carries the
  child SHAs in order; `load` parses them and dereferences recursively.

### Tag conventions

- `option`: `None=0` (0 children), `Some=1` (1 child).
- `result`: `Ok=0` (1 child), `Error=1` (1 child).
- `list` (flat): `tag=0`, `n`=length.
- `pair`/`triple`/`quad`: `tag=0`, `n`=arity.
- User variants (PPX): tag = declaration-order constructor index.
- User records (PPX): `tag=0`, `n`=field count.

## Modules

- **`Store`** — connection lifecycle + effect GADT + sqlite-backed handler.
  Exposes `type t`, `open_cdn`/`close_cdn`/`with_cdn`/`with_handlers`, the
  effect constructors `Content_put/Get`, `Ensure_cache`,
  `Cache_put/Get/Delete`, and direct `perform` wrappers.

- **`SerDe`** — `type 'a t = { store : 'a -> bytes; load : bytes -> 'a }`.
  Primitives built via `of_bin_prot : 'a Bin_prot.Type_class.t -> 'a t`.
  Polymorphic combinators: `option`, `list`, `result`, `pair`, `triple`,
  `quad`. Submodule `Merkle` with `store ~tag children`, `load sha`.

- **`Cache`** — `module type S` (table_name, key, value, key_serde,
  value_serde) + `Make (C : S)` functor producing `ensure_table`, `put`,
  `get_opt`, `delete`.

- **`Memo`** — `Memo.Make (C : Cache.S)` with `memoize{1..4}` and partial
  variants `memoize{2..4}_keyed{1..N-1}`.

## PPX (package `ppx_camldn`)

- `[@@deriving camldn]` via `ppx_deriving` plugin API — generates
  `serde_<t>` of type `<t> SerDe.t` for records and variants.
- `let%memo[@cache M][@key args...] f = …` via `Ppxlib.Extension.V3` —
  expands to `Memo.Make(M).memoizeN_keyedK` with the correct arity.

## Build / Test

```
make build  # → nix develop --no-pure-eval -c dune build
make test   # → nix develop --no-pure-eval -c dune runtest
make shell  # enter the nix dev shell
```

After bumping deps in `dune-project`, run `direnv reload` to refresh the
nix-built shell.

## Known limitations (v0)

- 64-bit platforms only (OCaml `int` stored via `bin_int` ≡ 63-bit tagged).
- No TTL / eviction — caches grow monotonically.
- A Domain that exits before `close_cdn` leaks its sqlite handle until close.
- Effect handlers are per-Domain; `Domain.spawn` inside `with_handlers`
  requires the child to reinstall them.

See `/Users/r2cuser/.claude/plans/this-is-camldn-a-fluffy-quokka.md` for the
full design reasoning.
