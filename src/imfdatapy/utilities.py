def make_key_str(key) -> str:
    parts = []
    for group in key:
        if group is None:
            parts.append('')
            continue

        elif isinstance(group, str):
            parts.append(group)  # single string
            continue

        elif (hasattr(group, '__len__') and len(group) == 0):
            parts.append('') 
            continue

        else:
            # Assume it's an iterable (list, tuple, R vector, etc.)
            items = []
            for x in group:
                if x is None:
                    continue
                sx = str(x)
                if sx == "" or sx.lower() == "null":
                    continue
                items.append(sx)

            parts.append("+".join(items) if items else "")
    return ".".join(parts)

def extract_dsd_object(msg):
    """Return the first DataStructureDefinition object from a StructureMessage."""
    for attr in ("datastructure", "metadatastructure", "structure", "_datastructure", "DataStructureDefinition"):
        container = getattr(msg, attr, None)
        if isinstance(container, dict) and container:
            return next(iter(container.values()))
    # Fallback: scan all objects
    for obj in msg.iter_objects():
        if obj.__class__.__name__.endswith("DataStructureDefinition"):
            return obj
    raise RuntimeError("No DataStructureDefinition found in StructureMessage.")


def resolve_codelist(ds, component):
    """
    ds: sdmx.StructureMessage (the one that contains codelists)
    component: a Dimension or DataAttribute object from the DSD
    -> returns the codelist object or None
    """
    # 1) Local representation
    lr = getattr(component, "local_representation", None)
    enum_ref = getattr(lr, "enumerated", None) if lr else None
    if enum_ref and getattr(enum_ref, "id", None) in ds.codelist:
        return ds.codelist[enum_ref.id]

    # 2) Concept's core representation
    concept = getattr(component, "concept_identity", None)
    cr = getattr(concept, "core_representation", None) if concept else None
    enum_ref = getattr(cr, "enumerated", None) if cr else None
    if enum_ref and getattr(enum_ref, "id", None) in ds.codelist:
        return ds.codelist[enum_ref.id]

    # 3) Heuristic: CL_<ID>
    guess_id = f"CL_{component.id}"
    if guess_id in ds.codelist:
        return ds.codelist[guess_id]

    return None