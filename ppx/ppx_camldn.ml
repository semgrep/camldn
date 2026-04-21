open Ppxlib
module B = Ast_builder.Default

let serde_name tname = "serde_" ^ tname

let rec elist ~loc = function
  | []      -> [%expr []]
  | x :: xs -> [%expr [%e x] :: [%e elist ~loc xs]]

let rec plist ~loc = function
  | []      -> [%pat? []]
  | x :: xs -> [%pat? [%p x] :: [%p plist ~loc xs]]

let serde_path ~loc name =
  B.pexp_ident ~loc
    (Loc.make ~loc (Longident.parse ("Camldn.SerDe." ^ name)))

let rec serde_of_core_type ct =
  let loc = ct.ptyp_loc in
  match ct.ptyp_desc with
  | Ptyp_constr ({ txt = Lident "int"; _ },    []) -> serde_path ~loc "int"
  | Ptyp_constr ({ txt = Lident "int64"; _ },  []) -> serde_path ~loc "int64"
  | Ptyp_constr ({ txt = Lident "string"; _ }, []) -> serde_path ~loc "string"
  | Ptyp_constr ({ txt = Lident "float"; _ },  []) -> serde_path ~loc "float"
  | Ptyp_constr ({ txt = Lident "bool"; _ },   []) -> serde_path ~loc "bool"
  | Ptyp_constr ({ txt = Lident "bytes"; _ },  []) -> serde_path ~loc "bytes"
  | Ptyp_constr ({ txt = Lident "unit"; _ },   []) -> serde_path ~loc "unit"
  | Ptyp_constr ({ txt = Lident "option"; _ }, [a]) ->
      [%expr [%e serde_path ~loc "option"] [%e serde_of_core_type a]]
  | Ptyp_constr ({ txt = Lident "list"; _ }, [a]) ->
      [%expr [%e serde_path ~loc "list"] [%e serde_of_core_type a]]
  | Ptyp_constr ({ txt = Lident "result"; _ }, [a; b]) ->
      [%expr [%e serde_path ~loc "result"]
               [%e serde_of_core_type a]
               [%e serde_of_core_type b]]
  | Ptyp_constr ({ txt = Lident name; _ }, []) ->
      B.evar ~loc (serde_name name)
  | Ptyp_tuple [a; b] ->
      [%expr [%e serde_path ~loc "pair"]
               [%e serde_of_core_type a] [%e serde_of_core_type b]]
  | Ptyp_tuple [a; b; c] ->
      [%expr [%e serde_path ~loc "triple"]
               [%e serde_of_core_type a]
               [%e serde_of_core_type b]
               [%e serde_of_core_type c]]
  | Ptyp_tuple [a; b; c; d] ->
      [%expr [%e serde_path ~loc "quad"]
               [%e serde_of_core_type a]
               [%e serde_of_core_type b]
               [%e serde_of_core_type c]
               [%e serde_of_core_type d]]
  | _ ->
      Location.raise_errorf ~loc
        "ppx_camldn: unsupported type in deriver (only primitives, \
         option/list/result, tuples of arity 2-4, and named types are supported)"

let expand_record ~loc tname fields =
  let sha_vars = List.mapi (fun i _ -> Printf.sprintf "_c%d" i) fields in
  let store_lambda =
    let store_calls =
      List.map2
        (fun (f : label_declaration) var ->
          let fname = f.pld_name.txt in
          let field_access =
            B.pexp_field ~loc (B.evar ~loc "r")
              (Loc.make ~loc (Longident.Lident fname))
          in
          let serde = serde_of_core_type f.pld_type in
          (var, [%expr [%e serde].store [%e field_access]]))
        fields sha_vars
    in
    let sha_list = elist ~loc (List.map (B.evar ~loc) sha_vars) in
    let body = [%expr Camldn.SerDe.Merkle.store ~tag:0 [%e sha_list]] in
    let body_with_lets =
      List.fold_right
        (fun (v, e) acc ->
          B.pexp_let ~loc Nonrecursive
            [ B.value_binding ~loc ~pat:(B.pvar ~loc v) ~expr:e ]
            acc)
        store_calls body
    in
    [%expr fun (r : [%t B.ptyp_constr ~loc
                        (Loc.make ~loc (Longident.Lident tname)) []]) ->
      [%e body_with_lets]]
  in
  let load_lambda =
    let sha_pat = plist ~loc (List.map (B.pvar ~loc) sha_vars) in
    let record_fields =
      List.map2
        (fun (f : label_declaration) var ->
          let serde = serde_of_core_type f.pld_type in
          ( Loc.make ~loc (Longident.Lident f.pld_name.txt)
          , [%expr [%e serde].load [%e B.evar ~loc var]] ))
        fields sha_vars
    in
    let record_expr = B.pexp_record ~loc record_fields None in
    [%expr fun _sha ->
      match Camldn.SerDe.Merkle.load _sha with
      | Some (0, [%p sha_pat]) -> [%e record_expr]
      | _ -> raise (Camldn.SerDe.Missing_content _sha)]
  in
  let body =
    [%expr { Camldn.SerDe.store = [%e store_lambda]
           ; load  = [%e load_lambda] }]
  in
  B.value_binding ~loc ~pat:(B.pvar ~loc (serde_name tname)) ~expr:body

let expand_variant ~loc tname ctors =
  let store_cases =
    List.mapi
      (fun tag_idx (cd : constructor_declaration) ->
        let cname = cd.pcd_name.txt in
        let cloc = cd.pcd_loc in
        match cd.pcd_args with
        | Pcstr_tuple [] ->
            B.case
              ~lhs:(B.ppat_construct ~loc:cloc
                      (Loc.make ~loc:cloc (Longident.Lident cname)) None)
              ~guard:None
              ~rhs:[%expr Camldn.SerDe.Merkle.store
                           ~tag:[%e B.eint ~loc tag_idx] []]
        | Pcstr_tuple args ->
            let arg_vars =
              List.mapi (fun i _ -> Printf.sprintf "_a%d" i) args
            in
            let pat_args =
              match arg_vars with
              | [ v ] -> B.pvar ~loc:cloc v
              | _ -> B.ppat_tuple ~loc:cloc (List.map (B.pvar ~loc:cloc) arg_vars)
            in
            let pat =
              B.ppat_construct ~loc:cloc
                (Loc.make ~loc:cloc (Longident.Lident cname))
                (Some pat_args)
            in
            let shas =
              List.map2
                (fun ty var ->
                  let serde = serde_of_core_type ty in
                  [%expr [%e serde].store [%e B.evar ~loc:cloc var]])
                args arg_vars
            in
            let sha_list = elist ~loc:cloc shas in
            B.case ~lhs:pat ~guard:None
              ~rhs:
                [%expr
                  Camldn.SerDe.Merkle.store ~tag:[%e B.eint ~loc:cloc tag_idx]
                    [%e sha_list]]
        | Pcstr_record _ ->
            Location.raise_errorf ~loc:cloc
              "ppx_camldn: inline records inside variants are not supported")
      ctors
  in
  let load_cases =
    List.mapi
      (fun tag_idx (cd : constructor_declaration) ->
        let cname = cd.pcd_name.txt in
        let cloc = cd.pcd_loc in
        match cd.pcd_args with
        | Pcstr_tuple [] ->
            B.case
              ~lhs:[%pat? Some ([%p B.pint ~loc:cloc tag_idx], [])]
              ~guard:None
              ~rhs:(B.pexp_construct ~loc:cloc
                      (Loc.make ~loc:cloc (Longident.Lident cname)) None)
        | Pcstr_tuple args ->
            let arg_vars =
              List.mapi (fun i _ -> Printf.sprintf "_a%d" i) args
            in
            let pat_list = plist ~loc:cloc (List.map (B.pvar ~loc:cloc) arg_vars) in
            let lhs =
              [%pat? Some ([%p B.pint ~loc:cloc tag_idx], [%p pat_list])]
            in
            let load_exprs =
              List.map2
                (fun ty var ->
                  let serde = serde_of_core_type ty in
                  [%expr [%e serde].load [%e B.evar ~loc:cloc var]])
                args arg_vars
            in
            let ctor_arg =
              match load_exprs with
              | [ e ] -> e
              | _ -> B.pexp_tuple ~loc:cloc load_exprs
            in
            B.case ~lhs ~guard:None
              ~rhs:(B.pexp_construct ~loc:cloc
                      (Loc.make ~loc:cloc (Longident.Lident cname))
                      (Some ctor_arg))
        | Pcstr_record _ ->
            Location.raise_errorf ~loc:cloc
              "ppx_camldn: inline records inside variants are not supported")
      ctors
  in
  let fallback =
    B.case ~lhs:[%pat? _] ~guard:None
      ~rhs:[%expr raise (Camldn.SerDe.Missing_content _sha)]
  in
  let _ = tname in
  let store_lambda =
    [%expr fun _x ->
      [%e B.pexp_match ~loc [%expr _x] store_cases]]
  in
  let load_lambda =
    [%expr fun _sha ->
      [%e
        B.pexp_match ~loc
          [%expr Camldn.SerDe.Merkle.load _sha]
          (load_cases @ [ fallback ])]]
  in
  let body =
    [%expr { Camldn.SerDe.store = [%e store_lambda]
           ; load  = [%e load_lambda] }]
  in
  B.value_binding ~loc ~pat:(B.pvar ~loc (serde_name tname)) ~expr:body

let expand_one_type_decl (td : type_declaration) =
  let loc = td.ptype_loc in
  let tname = td.ptype_name.txt in
  if td.ptype_params <> [] then
    Location.raise_errorf ~loc
      "ppx_camldn: parameterized types are not supported in the deriver \
       (define a monomorphic wrapper instead)";
  let vb =
    match td.ptype_kind with
    | Ptype_record fields -> expand_record ~loc tname fields
    | Ptype_variant ctors -> expand_variant ~loc tname ctors
    | _ ->
        Location.raise_errorf ~loc
          "ppx_camldn: only records and variants are supported"
  in
  let type_annot =
    B.ptyp_constr ~loc
      (Loc.make ~loc (Longident.parse "Camldn.SerDe.t"))
      [ B.ptyp_constr ~loc (Loc.make ~loc (Longident.Lident tname)) [] ]
  in
  let pat = B.ppat_constraint ~loc (B.pvar ~loc (serde_name tname)) type_annot in
  { vb with pvb_pat = pat }

let rec core_type_uses_name ct names =
  match ct.ptyp_desc with
  | Ptyp_constr ({ txt = Lident n; _ }, args) ->
      List.mem n names || List.exists (fun t -> core_type_uses_name t names) args
  | Ptyp_tuple tys -> List.exists (fun t -> core_type_uses_name t names) tys
  | _ -> false

let type_decl_is_recursive (td : type_declaration) names =
  match td.ptype_kind with
  | Ptype_record fields ->
      List.exists (fun f -> core_type_uses_name f.pld_type names) fields
  | Ptype_variant ctors ->
      List.exists (fun c ->
        match c.pcd_args with
        | Pcstr_tuple args ->
            List.exists (fun t -> core_type_uses_name t names) args
        | Pcstr_record fields ->
            List.exists (fun f -> core_type_uses_name f.pld_type names) fields) ctors
  | _ -> false

let group_rec_flag type_decls =
  let names = List.map (fun td -> td.ptype_name.txt) type_decls in
  if List.exists (fun td -> type_decl_is_recursive td names) type_decls
  then Recursive
  else Nonrecursive

let expand_str ~options:_ ~path:_ type_decls =
  let loc =
    match type_decls with td :: _ -> td.ptype_loc | [] -> Location.none
  in
  let bindings = List.map expand_one_type_decl type_decls in
  let rec_flag = group_rec_flag type_decls in
  [ B.pstr_value ~loc rec_flag bindings ]

let expand_sig ~options:_ ~path:_ type_decls =
  List.map
    (fun td ->
      let loc = td.ptype_loc in
      let tname = td.ptype_name.txt in
      let ct = Ppx_deriving.core_type_of_type_decl td in
      let ty =
        B.ptyp_constr ~loc
          (Loc.make ~loc (Longident.parse "Camldn.SerDe.t"))
          [ ct ]
      in
      B.psig_value ~loc
        (B.value_description ~loc
           ~name:(Loc.make ~loc (serde_name tname))
           ~type_:ty ~prim:[]))
    type_decls

let deriver =
  Ppx_deriving.create "camldn" ~type_decl_str:expand_str
    ~type_decl_sig:expand_sig ()

(* --- let%memo extension ---

   Syntax:
     let%memo[@cache MyCache] [@key a b] f (a : ta) (b : tb) (c : tc) : tv = ...

   Expands to:
     let f =
       let module M = Camldn.Memo.Make (MyCache) in
       M.memoizeN_keyedK ~key:(fun a b -> (a, b)) ~body:(fun a b c -> ...)

   [@cache M] is required. [@key a b ...] lists which positional args are in
   the cache key (default: all args).
*)

let find_attr name attrs =
  List.find_opt (fun a -> String.equal a.attr_name.txt name) attrs

let payload_as_module_ident ~loc (p : payload) =
  match p with
  | PStr [ { pstr_desc =
      Pstr_eval
        ( { pexp_desc = Pexp_construct ({ txt = lid; _ }, None); _ }
        , _ )
      ; _ } ] -> lid
  | _ ->
      Location.raise_errorf ~loc
        "[@cache M]: payload must be a module identifier"

let payload_as_ident_list ~loc (p : payload) : string list =
  let rec collect_exp e =
    match e.pexp_desc with
    | Pexp_ident { txt = Lident name; _ } -> [ name ]
    | Pexp_apply
        ( { pexp_desc = Pexp_ident { txt = Lident name; _ }; _ }
        , rest ) ->
        name :: List.concat_map (fun (_, e) -> collect_exp e) rest
    | Pexp_tuple es -> List.concat_map collect_exp es
    | _ ->
        Location.raise_errorf ~loc
          "[@key a b …]: payload must be a list of identifiers"
  in
  match p with
  | PStr [ { pstr_desc = Pstr_eval (e, _); _ } ] -> collect_exp e
  | _ ->
      Location.raise_errorf ~loc
        "[@key a b …]: payload must be a sequence of identifiers"

let peel_args body =
  let rec loop acc e =
    match e.pexp_desc with
    | Pexp_fun (Nolabel, None, pat, rest) ->
        (match pat.ppat_desc with
         | Ppat_var { txt; _ } -> loop ((txt, None) :: acc) rest
         | Ppat_constraint ({ ppat_desc = Ppat_var { txt; _ }; _ }, ty) ->
             loop ((txt, Some ty) :: acc) rest
         | _ ->
             Location.raise_errorf ~loc:pat.ppat_loc
               "let%%memo: only simple [arg] or [(arg : ty)] patterns are supported")
    | Pexp_constraint (inner, _) -> loop acc inner
    | _ -> List.rev acc, e
  in
  loop [] body

let mk_ident ~loc name =
  B.pexp_ident ~loc (Loc.make ~loc (Longident.Lident name))

let memoize_name ~n_args ~n_key =
  if n_args = n_key then Printf.sprintf "memoize%d" n_args
  else Printf.sprintf "memoize%d_keyed%d" n_args n_key

let expand_let_memo_binding ~loc (vb : value_binding) =
  let attrs = vb.pvb_attributes in
  let cache_mod =
    match find_attr "cache" attrs with
    | None ->
        Location.raise_errorf ~loc
          "let%%memo: missing [@cache M] attribute naming the Cache.S module"
    | Some a -> payload_as_module_ident ~loc a.attr_payload
  in
  let fname =
    match vb.pvb_pat.ppat_desc with
    | Ppat_var { txt; _ } -> txt
    | Ppat_constraint ({ ppat_desc = Ppat_var { txt; _ }; _ }, _) -> txt
    | _ ->
        Location.raise_errorf ~loc:vb.pvb_pat.ppat_loc
          "let%%memo: only simple named bindings are supported"
  in
  let args, body = peel_args vb.pvb_expr in
  if args = [] then
    Location.raise_errorf ~loc "let%%memo: function must take at least 1 argument";
  let n_args = List.length args in
  if n_args > 4 then
    Location.raise_errorf ~loc
      "let%%memo: arity > 4 not supported — pass a tuple instead";
  let key_args =
    match find_attr "key" attrs with
    | None -> List.map fst args
    | Some a -> payload_as_ident_list ~loc a.attr_payload
  in
  List.iter
    (fun kn ->
      if not (List.mem_assoc kn args) then
        Location.raise_errorf ~loc
          "let%%memo: [@key] references unknown argument %S" kn)
    key_args;
  let n_key = List.length key_args in
  if n_key = 0 then
    Location.raise_errorf ~loc
      "let%%memo: [@key] must reference at least one argument";
  if n_key > n_args then
    Location.raise_errorf ~loc
      "let%%memo: [@key] references more args than the function takes";
  let memo_fn = memoize_name ~n_args ~n_key in
  let arg_patterns =
    List.map
      (fun (name, ty_opt) ->
        match ty_opt with
        | None -> B.pvar ~loc name
        | Some ty -> B.ppat_constraint ~loc (B.pvar ~loc name) ty)
      args
  in
  let body_lambda =
    List.fold_right
      (fun pat acc -> B.pexp_fun ~loc Nolabel None pat acc)
      arg_patterns body
  in
  let key_lambda =
    let key_pats =
      List.map
        (fun kn ->
          match List.assoc kn args with
          | None -> B.pvar ~loc kn
          | Some ty -> B.ppat_constraint ~loc (B.pvar ~loc kn) ty)
        key_args
    in
    let key_body =
      match key_args with
      | [ a ] -> mk_ident ~loc a
      | _ -> B.pexp_tuple ~loc (List.map (mk_ident ~loc) key_args)
    in
    List.fold_right
      (fun pat acc -> B.pexp_fun ~loc Nolabel None pat acc)
      key_pats key_body
  in
  let cache_mod_expr = B.pmod_ident ~loc (Loc.make ~loc cache_mod) in
  let apply_memoize =
    B.pexp_apply ~loc
      (B.pexp_ident ~loc
         (Loc.make ~loc (Longident.Ldot (Lident "M", memo_fn))))
      [ Labelled "key", key_lambda; Labelled "body", body_lambda ]
  in
  let body_with_module =
    B.pexp_letmodule ~loc
      (Loc.make ~loc (Some "M"))
      (B.pmod_apply ~loc
         (B.pmod_ident ~loc
            (Loc.make ~loc (Longident.parse "Camldn.Memo.Make")))
         cache_mod_expr)
      apply_memoize
  in
  B.value_binding ~loc ~pat:(B.pvar ~loc fname) ~expr:body_with_module

let expand_memo ~loc rec_flag bindings =
  let new_bindings = List.map (expand_let_memo_binding ~loc) bindings in
  B.pstr_value ~loc rec_flag new_bindings

let memo_extension =
  Extension.declare
    "memo"
    Extension.Context.structure_item
    Ast_pattern.(pstr (pstr_value __ __ ^:: nil))
    (fun ~loc ~path:_ rec_flag bindings -> expand_memo ~loc rec_flag bindings)

(* Attribute-based syntax:
     let parse_with_opts (src : string) (strict : bool) : int = body
     [@cache Parse_cache][@key src]
   The user's [@cache ...] / [@key ...] attach to the body expression (since
   they appear after the RHS). We walk the function/constraint wrappers,
   pull them up to the binding level, and reuse [expand_let_memo_binding]. *)

let is_memo_attr (a : attribute) =
  let n = a.attr_name.txt in
  String.equal n "cache" || String.equal n "key"

let rec pull_memo_attrs (e : expression) : expression * attribute list =
  let pulled, kept = List.partition is_memo_attr e.pexp_attributes in
  let e = { e with pexp_attributes = kept } in
  match e.pexp_desc with
  | Pexp_fun (l, d, p, body) ->
      let body', more = pull_memo_attrs body in
      { e with pexp_desc = Pexp_fun (l, d, p, body') }, pulled @ more
  | Pexp_constraint (inner, ty) ->
      let inner', more = pull_memo_attrs inner in
      { e with pexp_desc = Pexp_constraint (inner', ty) }, pulled @ more
  | Pexp_newtype (tv, inner) ->
      let inner', more = pull_memo_attrs inner in
      { e with pexp_desc = Pexp_newtype (tv, inner') }, pulled @ more
  | Pexp_sequence (a, b) ->
      let b', more = pull_memo_attrs b in
      { e with pexp_desc = Pexp_sequence (a, b') }, pulled @ more
  | _ -> e, pulled

let rewrite_vb_if_memo (vb : value_binding) =
  let expr', pulled = pull_memo_attrs vb.pvb_expr in
  match pulled with
  | [] -> vb
  | _ ->
      let vb' =
        { vb with
          pvb_expr = expr'
        ; pvb_attributes = vb.pvb_attributes @ pulled }
      in
      expand_let_memo_binding ~loc:vb.pvb_loc vb'

let memo_attr_mapper =
  object
    inherit Ppxlib.Ast_traverse.map as super

    method! structure_item item =
      let item = super#structure_item item in
      match item.pstr_desc with
      | Pstr_value (rec_flag, bindings) ->
          let bindings' = List.map rewrite_vb_if_memo bindings in
          { item with pstr_desc = Pstr_value (rec_flag, bindings') }
      | _ -> item
  end

let () =
  Ppx_deriving.register deriver;
  Driver.register_transformation "ppx_camldn"
    ~extensions:[ memo_extension ]
    ~impl:memo_attr_mapper#structure
