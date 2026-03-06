{ inputs, pkgs, ... }:

let
  system = pkgs.stdenv.system;
  on = inputs.opam-nix.lib.${system};
  localPackagesQuery = builtins.mapAttrs (_: pkgs.lib.last) (on.listRepo (on.makeOpamRepo ./.));
  repos = [ "${inputs.opam-repository}" ];

  devPackagesQuery = {
    ocaml-lsp-server = "*";
    ocamlformat = "*";
  };
  query = devPackagesQuery // {
    ocaml-base-compiler = "*";
  };

  scope = on.buildOpamProject' { inherit repos; } ./. query;
  overlay = final: prev: {
    # You can add overrides here
  };
  scope' = scope.overrideScope overlay;

  # Packages from devPackagesQuery
  devPackages = builtins.attrValues (pkgs.lib.getAttrs (builtins.attrNames devPackagesQuery) scope');
  # Packages in this workspace
  packages = pkgs.lib.getAttrs (builtins.attrNames localPackagesQuery) scope';

in
{
  packages = packages ++ devPackages;

  scripts.build.exec = "dune build";
  scripts.test.exec = "dune test";
}
