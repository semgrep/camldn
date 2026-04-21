exception Missing_content of bytes

type 'a t = {
  store : 'a -> bytes;
  load  : bytes -> 'a;
}

let sha256 b =
  Digestif.SHA256.(digest_bytes b |> to_raw_string) |> Bytes.of_string

let bytes_of_buf buf =
  let len = Bin_prot.Common.buf_len buf in
  let b = Bytes.create len in
  Bin_prot.Common.blit_buf_bytes buf b ~len;
  b

let buf_of_bytes b =
  let len = Bytes.length b in
  let buf = Bin_prot.Common.create_buf len in
  Bin_prot.Common.blit_bytes_buf b buf ~len;
  buf

let of_bin_prot (type a) (tc : a Bin_prot.Type_class.t) : a t =
  { store =
      (fun x ->
        let buf = Bin_prot.Utils.bin_dump tc.writer x in
        let content = bytes_of_buf buf in
        let sha = sha256 content in
        Store.content_put sha content;
        sha)
  ; load =
      (fun sha ->
        match Store.content_get sha with
        | None -> raise (Missing_content sha)
        | Some content ->
            let buf = buf_of_bytes content in
            let pos_ref = ref 0 in
            tc.reader.read buf ~pos_ref)
  }

let int    = of_bin_prot Bin_prot.Type_class.bin_int
let int64  = of_bin_prot Bin_prot.Type_class.bin_int64
let string = of_bin_prot Bin_prot.Type_class.bin_string
let float  = of_bin_prot Bin_prot.Type_class.bin_float
let bool   = of_bin_prot Bin_prot.Type_class.bin_bool
let unit   = of_bin_prot Bin_prot.Type_class.bin_unit

let bytes : bytes t = {
  store = (fun b -> string.store (Bytes.to_string b));
  load  = (fun sha -> Bytes.of_string (string.load sha));
}

module Merkle = struct
  let version = '\x01'
  let header_len = 1 + 2 + 4
  let sha_len = 32

  let store ~tag children =
    if tag < 0 || tag > 0xFFFF then
      invalid_arg "Camldn.SerDe.Merkle.store: tag out of uint16 range";
    let n = List.length children in
    let total = header_len + sha_len * n in
    let b = Bytes.create total in
    Bytes.set b 0 version;
    Bytes.set_int16_le b 1 tag;
    Bytes.set_int32_le b 3 (Int32.of_int n);
    List.iteri
      (fun i child ->
        if Bytes.length child <> sha_len then
          invalid_arg
            "Camldn.SerDe.Merkle.store: child SHA must be 32 bytes";
        Bytes.blit child 0 b (header_len + i * sha_len) sha_len)
      children;
    let sha = sha256 b in
    Store.content_put sha b;
    sha

  let load sha =
    match Store.content_get sha with
    | None -> None
    | Some b ->
        if Bytes.length b < header_len || Bytes.get b 0 <> version then None
        else
          let tag = Bytes.get_uint16_le b 1 in
          let n = Int32.to_int (Bytes.get_int32_le b 3) in
          let expected = header_len + sha_len * n in
          if n < 0 || Bytes.length b <> expected then None
          else
            let children =
              List.init n (fun i ->
                Bytes.sub b (header_len + i * sha_len) sha_len)
            in
            Some (tag, children)
end

let option inner = {
  store = (function
    | None   -> Merkle.store ~tag:0 []
    | Some x -> Merkle.store ~tag:1 [inner.store x]);
  load = (fun sha ->
    match Merkle.load sha with
    | Some (0, [])  -> None
    | Some (1, [c]) -> Some (inner.load c)
    | _             -> raise (Missing_content sha));
}

let list inner = {
  store = (fun xs -> Merkle.store ~tag:0 (List.map inner.store xs));
  load  = (fun sha ->
    match Merkle.load sha with
    | Some (0, cs) -> List.map inner.load cs
    | _            -> raise (Missing_content sha));
}

let result a b = {
  store = (function
    | Ok x    -> Merkle.store ~tag:0 [a.store x]
    | Error e -> Merkle.store ~tag:1 [b.store e]);
  load = (fun sha ->
    match Merkle.load sha with
    | Some (0, [c]) -> Ok    (a.load c)
    | Some (1, [c]) -> Error (b.load c)
    | _             -> raise (Missing_content sha));
}

let pair a b = {
  store = (fun (x, y) -> Merkle.store ~tag:0 [a.store x; b.store y]);
  load  = (fun sha ->
    match Merkle.load sha with
    | Some (0, [cx; cy]) -> (a.load cx, b.load cy)
    | _                  -> raise (Missing_content sha));
}

let triple a b c = {
  store = (fun (x, y, z) ->
    Merkle.store ~tag:0 [a.store x; b.store y; c.store z]);
  load = (fun sha ->
    match Merkle.load sha with
    | Some (0, [cx; cy; cz]) -> (a.load cx, b.load cy, c.load cz)
    | _                      -> raise (Missing_content sha));
}

let quad a b c d = {
  store = (fun (w, x, y, z) ->
    Merkle.store ~tag:0
      [a.store w; b.store x; c.store y; d.store z]);
  load = (fun sha ->
    match Merkle.load sha with
    | Some (0, [cw; cx; cy; cz]) ->
        (a.load cw, b.load cx, c.load cy, d.load cz)
    | _ -> raise (Missing_content sha));
}
