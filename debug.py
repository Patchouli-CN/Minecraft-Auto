from simmc.utils.tiny_mapper import TinyMapper

mapper = TinyMapper(r"D:\python_play\SIMMC_proj\myagent\mappings\mappings.tiny")

info = mapper.get_class_info_by_readable("ClientWorld")

for m in info.methods:
    print(m.to_pysig())
