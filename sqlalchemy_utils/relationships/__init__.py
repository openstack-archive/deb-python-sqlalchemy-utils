from .chained_join import chained_join  # noqa
from .select_aggregate import select_aggregate  # noqa


def path_to_relationships(path, cls):
    relationships = []
    for path_name in path.split('.'):
        rel = getattr(cls, path_name)
        relationships.append(rel)
        cls = rel.mapper.class_
    return relationships
