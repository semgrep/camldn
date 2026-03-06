(lang dune 3.0)

(name camldn)

(source
 (github semgrep/camldn))

(generate_opam_files true)

(authors "Austin Theriault <austin@cutedogs.org>")

(maintainers "Austin Theriault <austin@cutedogs.org>")

(license LICENSE)

(package
 (name camldn)
 (synopsis "Cache computations to SQLite3 using algebraic effect handlers")
 (depends
  (ocaml (>= 5.3))
  dune
  (sqlite3 (>= 5.1.0))
  (uri (>= 4.0.0))
  (testo :with-test)
   (ocaml-lsp-server :with-dev-setup)
   (ocamlformat :with-dev-setup)
   (ocamlmerlin-mlx :with-dev-setup)
))

(package
 (name ppx_camldn)
 (synopsis "PPX for automatic caching via camldn")
 (depends
  (ocaml (>= 5.3))
  dune
  (ppxlib (>= 0.35.0))
  (camldn :with-test)
  (testo :with-test)))
