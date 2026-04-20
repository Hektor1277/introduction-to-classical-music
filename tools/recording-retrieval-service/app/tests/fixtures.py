from __future__ import annotations


def sample_request(item_count: int = 1) -> dict:
    items = []
    for index in range(item_count):
        item_id = f"recording-{index + 1}"
        items.append(
            {
                "itemId": item_id,
                "recordingId": item_id,
                "workId": f"work-{index + 1}",
                "composerId": f"composer-{index + 1}",
                "workTypeHint": "orchestral",
                "sourceLine": f"Conductor {index + 1} | Orchestra {index + 1} | 197{index} | live",
                "seed": {
                    "title": f"Recording {index + 1}",
                    "composerName": "贝多芬",
                    "composerNameLatin": "Ludwig van Beethoven",
                    "workTitle": "第五交响曲",
                    "workTitleLatin": "Symphony No. 5",
                    "catalogue": "Op.67",
                    "performanceDateText": "1975",
                    "venueText": "",
                    "albumTitle": "",
                    "label": "",
                    "releaseDate": "",
                    "credits": [
                        {"role": "conductor", "displayName": f"Conductor {index + 1}", "label": f"Conductor {index + 1}"},
                        {"role": "orchestra", "displayName": f"Orchestra {index + 1}", "label": f"Orchestra {index + 1}"},
                    ],
                    "links": [],
                    "notes": "",
                },
                "requestedFields": [
                    "links",
                    "performanceDateText",
                    "venueText",
                    "albumTitle",
                    "label",
                    "releaseDate",
                    "notes",
                    "images",
                ],
            }
        )

    return {
        "requestId": "owner-generated-uuid",
        "source": {
            "kind": "owner-entity-check",
            "ownerRunId": "run-1",
            "requestedBy": "owner-tool",
        },
        "items": items,
        "options": {
            "maxConcurrency": 2,
            "timeoutMs": 3000,
            "returnPartialResults": True,
        },
    }
