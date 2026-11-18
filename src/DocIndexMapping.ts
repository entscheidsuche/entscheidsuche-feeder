// Mapping der Spider-Indizes (entscheidsuche.v2-<spider>).
//
// Jeder Weg, auf dem ein solcher Index entsteht, muss ihn mit diesem Mapping anlegen:
// der Import (ChunkProcessor.createDocIndex) ebenso wie der SpiderProcessor nach einem
// Neuaufbau (jobtyp=neu) oder bei einem neuen Spider. Ohne explizites Mapping legt
// Elasticsearch den Index beim ersten Dokument automatisch an und macht aus id,
// hierarchy, canton, reference und attachment.language Textfelder. Dann scheitern die
// Sortierung nach id (Bestandsscan des Feeders, Abgleich des Konsolidierers, Suche des
// Frontends) und die Aggregationen/Filter auf hierarchy.
export const DOC_INDEX_MAPPING = {
    "mappings": {
        "dynamic": "true",
        "dynamic_date_formats": [
            "strict_date_optional_time",
            "yyyy/MM/dd HH:mm:ss Z||yyyy/MM/dd Z"
        ],
        "dynamic_templates": [],
        "date_detection": true,
        "numeric_detection": false,
        "properties": {
            "abstract": {
                "properties": {
                    "de": {
                        "type": "text"
                    },
                    "fr": {
                        "type": "text"
                    },
                    "it": {
                        "type": "text"
                    }
                }
            },
            "attachment": {
                "properties": {
                    "author": {
                        "type": "text"
                    },
                    "content": {
                        "type": "text",
                        "store": true
                    },
                    "content_length": {
                        "type": "long"
                    },
                    "content_type": {
                        "type": "keyword"
                    },
                    "content_url": {
                        "type": "keyword"
                    },
                    "date": {
                        "type": "date"
                    },
                    "language": {
                        "type": "keyword"
                    },
                    "source": {
                        "type": "keyword"
                    },
                    "title": {
                        "type": "text"
                    }
                }
            },
            "canton": {
                "type": "keyword"
            },
            "date": {
                "type": "date"
            },
            "hierarchy": {
                "type": "keyword"
            },
            "id": {
                "type": "keyword"
            },
            "meta": {
                "properties": {
                    "de": {
                        "type": "text"
                    },
                    "fr": {
                        "type": "text"
                    },
                    "it": {
                        "type": "text"
                    }
                }
            },
            "reference": {
                "type": "keyword"
            },
            "scrapedate": {
                "type": "date"
            },
            "source": {
                "type": "keyword"
            },
            "title": {
                "dynamic": "true",
                "properties": {
                    "de": {
                        "type": "text"
                    },
                    "fr": {
                        "type": "text"
                    },
                    "it": {
                        "type": "text"
                    }
                }
            }
        }
    }
};
