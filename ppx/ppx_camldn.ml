open Ppxlib

(** Attribute: [@@cache ModuleName] on value bindings.
    The payload is a module path (uppercase ident parses as a constructor). *)
let cache_attr =
  Attribute.declare "cache" Attribute.Context.Value_binding
    Ast_pattern.(single_expr_payload (pexp_construct __ none))
    (fun lid -> lid)

(** Extract a simple variable name from a pattern, handling both
    [Ppat_var] and [Ppat_constraint(Ppat_var, _)]. *)
let extract_var_name pat =
  match pat.ppat_desc with
  | Ppat_var { txt; _ } -> Some txt
  | Ppat_constraint ({ ppat_desc = Ppat_var { txt; _ }; _ }, _) -> Some txt
  | _ -> None

(** Transform a single value binding that carries [@@cache M].
    [module_lid] is the longident from the attribute payload.
    [qualified_prefix] is the dot-separated module path for [f_name]. *)
let transform_vb ~loc ~module_lid ~qualified_prefix vb =
  (* 1. Get the function name *)
  let func_name =
    match extract_var_name vb.pvb_pat with
    | Some name -> name
    | None ->
        Location.raise_errorf ~loc:vb.pvb_pat.ppat_loc
          "[@@cache] can only be applied to named function bindings"
  in
  (* 2. Peel outermost Pexp_fun — we need exactly one argument *)
  let lbl, default, param_pat, body =
    match vb.pvb_expr.pexp_desc with
    | Pexp_fun (lbl, default, param_pat, body) -> (lbl, default, param_pat, body)
    | Pexp_constraint ({ pexp_desc = Pexp_fun (lbl, default, param_pat, body); _ }, _) ->
        (lbl, default, param_pat, body)
    | _ ->
        Location.raise_errorf ~loc:vb.pvb_expr.pexp_loc
          "[@@cache] can only be applied to function definitions"
  in
  (* 3. Reject multi-argument functions (body is another fun) *)
  let body_is_fun =
    match body.pexp_desc with
    | Pexp_fun _ -> true
    | Pexp_constraint ({ pexp_desc = Pexp_fun _; _ }, _) -> true
    | _ -> false
  in
  if body_is_fun then
    Location.raise_errorf ~loc:vb.pvb_expr.pexp_loc
      "[@@cache] only supports single-argument functions";
  (* 4. Extract the parameter variable name *)
  let param_var_name =
    match extract_var_name param_pat with
    | Some name -> name
    | None ->
        Location.raise_errorf ~loc:param_pat.ppat_loc
          "[@@cache] requires a simple named parameter"
  in
  (* 5. Build the new expression:
     fun param_pat ->
       let f__uncached = <original_pvb_expr> in
       M.maybe_do ~cache_name:"Path.f" ~f:f__uncached param_var *)
  let cache_name_str = qualified_prefix ^ "." ^ func_name in
  let uncached_name = func_name ^ "__uncached" in
  let maybe_do_ident =
    Ast_builder.Default.pexp_ident ~loc
      { txt = Ldot (module_lid, "maybe_do"); loc }
  in
  let param_var_expr =
    Ast_builder.Default.pexp_ident ~loc
      { txt = Lident param_var_name; loc }
  in
  let uncached_var_expr =
    Ast_builder.Default.pexp_ident ~loc
      { txt = Lident uncached_name; loc }
  in
  let new_body =
    [%expr
      let [%p Ast_builder.Default.ppat_var ~loc { txt = uncached_name; loc }] =
        [%e vb.pvb_expr]
      in
      [%e maybe_do_ident]
        ~cache_name:[%e Ast_builder.Default.estring ~loc cache_name_str]
        ~f:[%e uncached_var_expr]
        [%e param_var_expr]]
  in
  let new_expr =
    Ast_builder.Default.pexp_fun ~loc lbl default param_pat new_body
  in
  { vb with pvb_expr = new_expr }

(** Walk a structure, tracking module nesting for qualified names. *)
let rec transform_structure ~main_module ~module_path items =
  List.concat_map (transform_structure_item ~main_module ~module_path) items

and transform_structure_item ~main_module ~module_path item =
  match item.pstr_desc with
  | Pstr_value (rec_flag, vbs) ->
      let vbs' =
        List.map
          (fun vb ->
            match Attribute.consume cache_attr vb with
            | None -> vb
            | Some (vb, module_lid) ->
                let qualified_prefix =
                  String.concat "." (main_module :: module_path)
                in
                transform_vb ~loc:item.pstr_loc ~module_lid ~qualified_prefix
                  vb)
          vbs
      in
      [ { item with pstr_desc = Pstr_value (rec_flag, vbs') } ]
  | Pstr_module mb ->
      let inner_path =
        match mb.pmb_name.txt with
        | Some name -> module_path @ [ name ]
        | None -> module_path
      in
      let mb' =
        { mb with
          pmb_expr = transform_module_expr ~main_module ~module_path:inner_path mb.pmb_expr
        }
      in
      [ { item with pstr_desc = Pstr_module mb' } ]
  | Pstr_recmodule mbs ->
      let mbs' =
        List.map
          (fun mb ->
            let inner_path =
              match mb.pmb_name.txt with
              | Some name -> module_path @ [ name ]
              | None -> module_path
            in
            { mb with
              pmb_expr = transform_module_expr ~main_module ~module_path:inner_path mb.pmb_expr
            })
          mbs
      in
      [ { item with pstr_desc = Pstr_recmodule mbs' } ]
  | _ -> [ item ]

and transform_module_expr ~main_module ~module_path mexpr =
  match mexpr.pmod_desc with
  | Pmod_structure items ->
      let items' = transform_structure ~main_module ~module_path items in
      { mexpr with pmod_desc = Pmod_structure items' }
  | _ -> mexpr

let () =
  Driver.V2.register_transformation "ppx_camldn"
    ~impl:(fun ctxt str ->
      let main_module =
        Expansion_context.Base.code_path ctxt
        |> Code_path.main_module_name
      in
      transform_structure ~main_module ~module_path:[] str)
