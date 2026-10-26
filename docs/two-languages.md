# vigy: two languages, one intrinsic set

A vigy carries a typed `Language` (vigy-types): `tatara-lisp` (the default, and
every vigy registered before 2026-10-22) or `blue`. Nothing translates one into
the other: a tatara vigy runs on the hand-built tatara interpreter it always
ran on, and a blue vigy is loaded per tick through `blue-lang-host`
(`Program::builder(...).frame(...)`, host effects refused, the tree-walker's
default budget) — the frame is never looser than the tatara path.

The 30 intrinsics are ONE `#[host(prefix = "vigy_")] impl VigyHost` block.
Blue names are the tatara names with `-` spelled `_` (`vigy-has?` is
`vigy_has?`), forced by blue's identifier grammar; optional arguments are
trailing `Option` parameters. The tatara rows (`TATARA` in vigy-eval) only
adapt arguments and call the same methods, and
`the_blue_surface_is_the_tatara_surface_respelled` fails when the two lists
or their required arities disagree. A `HostExtension` reaches blue vigies
through `install_blue`, which defaults to `install`; register snake_case
names there for blue programs to call them.

## Status

Counted 2026-10-22: 70 tests across `vigy-types` (21), `vigy-store` (11),
`vigy-eval` (26), `vigy-runtime` (12). `examples/tear-mirror.tatara` does not
parse (an unmatched closing paren); a test pins that both paths refuse it alike.
