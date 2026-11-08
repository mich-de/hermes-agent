from datetime import datetime

from hermes import _changed_files, _due, _parse_subtasks, _parse_verdict

subs = _parse_subtasks('Ecco:\n[{"titolo": "API", "istruzioni": "scrivi endpoint"}, {"titolo": "", "istruzioni": "test"},'
                       ' {"istruzioni": ""}, 5]\nfine')
assert [s["title"] for s in subs] == ["API", "sottotask 2"], subs
assert _parse_subtasks("niente json") == []
assert _parse_subtasks("[rotto") == []
assert len(_parse_subtasks(str([{"istruzioni": "x"}] * 9).replace("'", '"'))) == 4

assert _parse_verdict("OK", 3) == {}
assert _parse_verdict("KO 2: manca il file\n- KO #3 – test assenti\nKO 9: fuori range", 3) == {2: "manca il file", 3: "test assenti"}

assert _changed_files({"a": 1, "b": 2}, {"a": 1, "b": 3, "c": 1}) == ["b", "c"]

now = datetime(2026, 11, 8, 9, 0)
assert _due({"time": "08:30", "last": ""}, now)
assert not _due({"time": "08:30", "last": "2026-11-08"}, now)
assert not _due({"time": "09:30", "last": ""}, now)
print("ok")
